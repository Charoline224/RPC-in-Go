package main

import (
	framing "RPCinGo/transport"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (s *Server) Invoke(ctx context.Context, serviceName string, methodName string, req proto.Message) (reply proto.Message, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.lock.RLock()
	srv, ok := s.serviceMap[serviceName]
	if !ok {
		s.lock.RUnlock()
		return nil, fmt.Errorf("service %s not found", serviceName)
	}
	method, ok := srv.Methods[methodName]
	s.lock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("method %s not found", methodName)
	}

	if req == nil ||
		reflect.ValueOf(req).IsNil() ||
		reflect.TypeOf(req) != method.ArgsType {
		return nil, errors.New("request type mismatch")
	}

	defer func() {
		if p := recover(); p != nil {
			reply = nil
			err = fmt.Errorf("RPC method panic: %v", p)
		}
	}()

	returnVals := method.Func.Call([]reflect.Value{
		srv.rcvr,
		reflect.ValueOf(ctx),
		reflect.ValueOf(req),
	})

	if !returnVals[1].IsNil() {
		return nil, returnVals[1].Interface().(error)
	}

	if returnVals[0].IsNil() {
		return nil, errors.New("RPC method returned nil response")
	}

	return returnVals[0].Interface().(proto.Message), nil
}

func (s *Server) NewRequest(serviceName, methodName string) (proto.Message, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	srv, ok := s.serviceMap[serviceName]
	if !ok {
		return nil, fmt.Errorf("service %s not found", serviceName)
	}

	method, ok := srv.Methods[methodName]
	if !ok {
		return nil, fmt.Errorf("method %s not found", methodName)
	}

	req := reflect.New(method.ArgsType.Elem())
	return req.Interface().(proto.Message), nil
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
	httpServer := &http.Server{
		Handler: s,
	}

	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	httpServer.Protocols = protocols

	return httpServer.Serve(s.listener)

}

func (s *Server) parseMethod(path string) (string, string, error) {
	// 标准 gRPC 路径：/package.Service/Method
	if !strings.HasPrefix(path, "/") {
		return "", "", errors.New("invalid RPC path")
	}

	parts := strings.Split(path[1:], "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("invalid RPC path")
	}

	return parts[0], parts[1], nil
}

func writeGRPCError(w http.ResponseWriter, code codes.Code, err error) {
	w.Header().Set("Grpc-Status", strconv.Itoa(int(code)))
	w.Header().Set("Grpc-Message", url.PathEscape(err.Error()))
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor != 2 {
		http.Error(w, "HTTP/2 required", http.StatusHTTPVersionNotSupported)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/grpc" &&
		contentType != "application/grpc+proto" {
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return
	}

	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Add("Trailer", "Grpc-Status")
	w.Header().Add("Trailer", "Grpc-Message")

	serviceName, methodName, err := s.parseMethod(r.URL.Path)
	if err != nil {
		writeGRPCError(w, codes.Unimplemented, err)
		return
	}
	req, err := s.NewRequest(serviceName, methodName)
	if err != nil {
		writeGRPCError(w, codes.Unimplemented, err)
		return
	}
	data, err := (framing.Framer{}).DecodeMessage(r.Body)
	if err != nil {
		code := codes.InvalidArgument
		if errors.Is(err, framing.ErrMessageTooLarge) {
			code = codes.ResourceExhausted
		}
		writeGRPCError(w, code, err)
		return
	}
	//检查是否有多余信息
	var extra [1]byte
	n, err := r.Body.Read(extra[:])
	if n > 0 {
		writeGRPCError(w, codes.InvalidArgument,
			errors.New("multiple messages in unary request"))
		return
	}
	if err != io.EOF {
		if err == nil {
			err = errors.New("unexpected empty read")
		}
		writeGRPCError(w, codes.InvalidArgument, err)
		return
	}

	if err := proto.Unmarshal(data, req); err != nil {
		writeGRPCError(w, codes.InvalidArgument, err)
		return
	}
	rep, err := s.Invoke(r.Context(), serviceName, methodName, req)
	if err != nil {
		var code codes.Code

		switch {
		case errors.Is(err, context.Canceled):
			code = codes.Canceled

		case errors.Is(err, context.DeadlineExceeded):
			code = codes.DeadlineExceeded

		default:
			code = status.Code(err)
		}

		writeGRPCError(w, code, err)
		return
	}
	repbyte, err := proto.Marshal(rep)
	if err != nil {
		writeGRPCError(w, codes.Internal, err)
		return
	}
	repmsg, err := (framing.Framer{}).EncodeMessage(repbyte)
	if err != nil {
		writeGRPCError(w, codes.Internal, err)
		return
	}
	if _, err := w.Write(repmsg); err != nil {
		return
	}
	w.Header().Set("Grpc-Status", "0")
}

// func (s *Server) handleConn(conn io.ReadWriteCloser) error {
// 	defer s.wg.Done()

// 	// 客户端 json.Encoder.Encode 会在 Option 后写入换行。
// 	reader := bufio.NewReader(conn)
// 	data, err := reader.ReadBytes('\n')
// 	if err != nil {
// 		return err
// 	}
// 	var option Option
// 	//反序列化取出option，不直接decode是因为decode的对象是从reader里面解析
// 	if err := json.Unmarshal(data, &option); err != nil {
// 		return err
// 	}
// 	//验证暗号
// 	if option.MagicNumber != DefaultOption.MagicNumber {
// 		return errors.New(" message doesnt match this server")
// 	}
// 	//构造编码器
// 	// 后续继续从同一个 reader 读取，保留可能预读的请求字节。
// 	codec := Codec.NewCodecFuncMap[option.CodecType](struct {
// 		io.Reader
// 		io.Writer
// 		io.Closer
// 	}{reader, conn, conn})
// 	//循环读请求
// 	for {
// 		var header Codec.Header
// 		err := codec.ReadHeader(&header)
// 		if err != nil {
// 			if errors.Is(err, io.EOF) {
// 				return nil
// 			}
// 			if header.Num == 0 {
// 				fmt.Println("rpc server: read header error:", err)
// 				continue
// 			}
// 			header.Error = err.Error()
// 			codec.Write(nil, &header)
// 			continue
// 		}
// 		//获取方法名->查找方法method,获取一堆调用需要的参数
// 		srvName := header.Service
// 		s.lock.RLock()
// 		srv, ok := s.serviceMap[srvName]
// 		s.lock.RUnlock()
// 		if !ok {
// 			if err := codec.ReadBody(nil); err != nil {
// 				return err
// 			}
// 			header.Error = fmt.Sprintf("service %s not found", srvName)
// 			codec.Write(nil, &header)
// 			continue
// 		}
// 		methName := header.Method
// 		s.lock.RLock()
// 		method, ok := srv.Methods[methName]
// 		s.lock.RUnlock()
// 		if !ok {
// 			if err := codec.ReadBody(nil); err != nil {
// 				return err
// 			}
// 			header.Error = fmt.Sprintf("method %s not found", methName)
// 			codec.Write(nil, &header)
// 			continue
// 		}
// 		res := reflect.New(method.ResType.Elem())
// 		args := reflect.New(method.ArgsType)
// 		err = codec.ReadBody(args.Interface())
// 		if err != nil {
// 			header.Error = err.Error()
// 			codec.Write(res.Interface(), &header)
// 			continue
// 		}
// 		//异步调用cli请求调用的函数
// 		s.wg.Add(1)
// 		go func(header Codec.Header, method Method, service reflect.Value, args, res reflect.Value) {
// 			defer s.wg.Done()
// 			returnVals := method.Func.Call([]reflect.Value{service, args.Elem(), res})
// 			err, ok := returnVals[0].Interface().(error) //类型断言
// 			if ok && err != nil {
// 				header.Error = err.Error()
// 				codec.Write(res.Interface(), &header)
// 				return
// 			}
// 			codec.Write(res.Interface(), &header)
// 		}(header, method, srv.rcvr, args, res)

// 	}
// }

// func (s *Server) Close() error {
// 	if s.close {
// 		return s.err
// 	}
// 	s.listener.Close()
// 	s.wg.Wait()
// 	return nil
// }
