package rpcingo

import (
	"RPCinGo/Codec"
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
	methodsMap map[string]Method
	lock       sync.RWMutex
	close      bool
	err        error
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
	return nil
}
func (s *Server) Listen(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		s.err = err
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn io.ReadWriteCloser) error {
	//解析option
	var option *Option
	json.NewDecoder(conn).Decode(&option)
	//验证暗号
	if option.MagicNumber != DefaultOption.MagicNumber {
		return errors.New(" message doesnt match this server")
	}
	//构造编码器
	codec := Codec.NewCodecFuncMap[option.CodecType](conn)
	//循环读请求
	var lock sync.Mutex
	for {
		var header Codec.Header
		err := codec.ReadHeader(&header)
		if err != nil {
			header.Error = err.Error()
			lock.Lock()
			codec.Write(nil, &header)
			lock.Unlock()
			return err
		}
		methName := header.ServiceMethod
		s.lock.RLock()
		method, ok := s.methodsMap[methName]
		s.lock.RUnlock()
		if !ok {
			header.Error = fmt.Sprintf("method %s not found", methName)
			lock.Lock()
			codec.Write(nil, &header)
			lock.Unlock()
			continue
		}

		res := reflect.New(method.ResType)
		service := method.Service
		args := reflect.New(method.ArgsType)

		err = codec.ReadBody(args.Interface())
		if err != nil {
			header.Error = err.Error()
			lock.Lock()
			codec.Write(res.Interface(), &header)
			lock.Unlock()
			continue
		}

		go func(header Codec.Header, method Method, service reflect.Value, args, res reflect.Value) {
			returnVals := method.Func.Call([]reflect.Value{service, args, res})
			err, ok := returnVals[0].Interface().(error) //类型断言
			if ok && err != nil {
				header.Error = err.Error()
				lock.Lock()
				codec.Write(res.Interface(), &header)
				lock.Unlock()
				return
			}
			lock.Lock()
			codec.Write(res.Interface(), &header)
			lock.Unlock()

		}(header, method, service, args, res)
	}
}
