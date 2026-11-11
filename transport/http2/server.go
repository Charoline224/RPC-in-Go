package main

import (
	"RPCinGo/Codec"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
)

type Service struct {
	Name    string
	typ     reflect.Type
	rcvr    reflect.Value
	Methods map[string]Method
}

type Method struct {
	Func     reflect.Value
	ArgsType reflect.Type
	ResType  reflect.Type
}

type Server struct {
	listener   net.Listener
	serviceMap map[string]Service
	lock       sync.RWMutex
	close      bool
	err        error
	wg         sync.WaitGroup
}

func NewServer() *Server {
	var s Server
	s.lock = sync.RWMutex{}
	s.wg = sync.WaitGroup{}
	s.serviceMap = make(map[string]Service)
	return &s
}

// 应该注册service而不是method
// 处理签名格式:func (c *Calculator) Add(
//
//	ctx context.Context,
//	req *pb.AddRequest,
//
// ) (*pb.AddResponse, error)
func (s *Server) Register(fullServiceName string, service interface{}) error {

	t := reflect.TypeOf(service)
	v := reflect.ValueOf(service)
	if t == nil || t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return errors.New("service must be a non-nil pointer")
	}
	if v.IsNil() {
		return errors.New("service pointer cannot be nil")
	}
	srv := Service{
		typ:     t,
		rcvr:    v,
		Name:    fullServiceName,
		Methods: make(map[string]Method),
	}
	if srv.Name == "" || !strings.Contains(srv.Name, ".") {
		return errors.New("invalid full service name")
	}

	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	errType := reflect.TypeOf((*error)(nil)).Elem()
	protoType := reflect.TypeOf((*proto.Message)(nil)).Elem()

	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		if method.Type.NumIn() != 3 ||
			method.Type.NumOut() != 2 ||

			method.Type.In(1) != ctxType ||
			method.Type.Out(1) != errType ||

			method.Type.Out(0).Kind() != reflect.Ptr ||
			method.Type.In(2).Kind() != reflect.Ptr {
			continue
		}
		if method.Type.In(2).Elem().Kind() != reflect.Struct ||
			method.Type.Out(0).Elem().Kind() != reflect.Struct {
			continue
		}
		// 请求和响应必须实现 proto.Message
		if !method.Type.In(2).Implements(protoType) ||
			!method.Type.Out(0).Implements(protoType) {
			continue
		}
		srv.Methods[method.Name] = Method{
			Func:     method.Func,
			ArgsType: method.Type.In(2),
			ResType:  method.Type.Out(0),
		}
	}
	if len(srv.Methods) == 0 {
		return fmt.Errorf("service %s has no valid RPC methods", srv.Name)
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.serviceMap[srv.Name]; exists {
		return fmt.Errorf("service %s already registered", srv.Name)
	}

	s.serviceMap[srv.Name] = srv
	return nil
}
func (s *Server) Listen(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		s.err = err
		return s.err
	}
	s.listener = listener
	return nil
}
func (s *Server) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil // 正常关闭，结束 Listen
			}
			return err
		}
		fmt.Println("server accepted connection from:", conn.RemoteAddr())
		s.wg.Add(1)
		go func(err error, conn io.ReadWriteCloser) {
			err = s.handleConn(conn)
			if err != nil {
				fmt.Println("handle connection error:", err)
			}
			conn.Close()
		}(err, conn)
	}

}

func (s *Server) handleConn(conn io.ReadWriteCloser) error {
	defer s.wg.Done()

	// 客户端 json.Encoder.Encode 会在 Option 后写入换行。
	reader := bufio.NewReader(conn)
	data, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	var option Option
	//反序列化取出option，不直接decode是因为decode的对象是从reader里面解析
	if err := json.Unmarshal(data, &option); err != nil {
		return err
	}
	//验证暗号
	if option.MagicNumber != DefaultOption.MagicNumber {
		return errors.New(" message doesnt match this server")
	}
	//构造编码器
	// 后续继续从同一个 reader 读取，保留可能预读的请求字节。
	codec := Codec.NewCodecFuncMap[option.CodecType](struct {
		io.Reader
		io.Writer
		io.Closer
	}{reader, conn, conn})
	//循环读请求
	for {
		var header Codec.Header
		err := codec.ReadHeader(&header)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if header.Num == 0 {
				fmt.Println("rpc server: read header error:", err)
				continue
			}
			header.Error = err.Error()
			codec.Write(nil, &header)
			continue
		}
		//获取方法名->查找方法method,获取一堆调用需要的参数
		srvName := header.Service
		s.lock.RLock()
		srv, ok := s.serviceMap[srvName]
		s.lock.RUnlock()
		if !ok {
			if err := codec.ReadBody(nil); err != nil {
				return err
			}
			header.Error = fmt.Sprintf("service %s not found", srvName)
			codec.Write(nil, &header)
			continue
		}
		methName := header.Method
		s.lock.RLock()
		method, ok := srv.Methods[methName]
		s.lock.RUnlock()
		if !ok {
			if err := codec.ReadBody(nil); err != nil {
				return err
			}
			header.Error = fmt.Sprintf("method %s not found", methName)
			codec.Write(nil, &header)
			continue
		}
		res := reflect.New(method.ResType.Elem())
		args := reflect.New(method.ArgsType)
		err = codec.ReadBody(args.Interface())
		if err != nil {
			header.Error = err.Error()
			codec.Write(res.Interface(), &header)
			continue
		}
		//异步调用cli请求调用的函数
		s.wg.Add(1)
		go func(header Codec.Header, method Method, service reflect.Value, args, res reflect.Value) {
			defer s.wg.Done()
			returnVals := method.Func.Call([]reflect.Value{service, args.Elem(), res})
			err, ok := returnVals[0].Interface().(error) //类型断言
			if ok && err != nil {
				header.Error = err.Error()
				codec.Write(res.Interface(), &header)
				return
			}
			codec.Write(res.Interface(), &header)
		}(header, method, srv.rcvr, args, res)

	}
}

func (s *Server) Close() error {
	if s.close {
		return s.err
	}
	s.listener.Close()
	s.wg.Wait()
	return nil
}
