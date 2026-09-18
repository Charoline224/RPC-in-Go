package main

import (
	"RPCinGo/Codec"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
)

type Method struct {
	Func     reflect.Value
	ArgsType reflect.Type
	ResType  reflect.Type
	Service  reflect.Value
}

type Server struct {
	listener   net.Listener
	methodsMap map[string]Method
	lock       sync.RWMutex
	close      bool
	err        error
	wg         sync.WaitGroup
}

func NewServer() *Server {
	var s Server
	s.lock = sync.RWMutex{}
	s.wg = sync.WaitGroup{}
	s.methodsMap = make(map[string]Method)
	return &s
}

func (s *Server) Register(service interface{}) error {
	t := reflect.TypeOf(service)
	s.lock.Lock()
	defer s.lock.Unlock()
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		if method.Type.NumIn() != 3 || method.Type.NumOut() != 1 || method.Type.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		var m Method
		m.Func = method.Func
		m.ArgsType = method.Type.In(1)
		m.ResType = method.Type.In(2)
		m.Service = reflect.ValueOf(service)
		s.methodsMap[method.Name] = m
	}
	fmt.Println("registered methods:", s.methodsMap)
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
		fmt.Println("server start reading request")
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
		methName := header.ServiceMethod
		fmt.Println("looking for method:", header.ServiceMethod)
		s.lock.RLock()
		method, ok := s.methodsMap[methName]
		s.lock.RUnlock()
		if !ok {
			header.Error = fmt.Sprintf("method %s not found", methName)
			codec.Write(nil, &header)
			continue
		}
		service := method.Service
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
		}(header, method, service, args, res)

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
