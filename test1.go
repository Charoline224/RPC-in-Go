package main

import (
	"RPCinGo/Codec"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const day1TestTimeout = 3 * time.Second

type day1Args struct {
	A int
	B int
}

type day1Reply struct {
	Value int
	Label string
}

type Day1ArithmeticService struct {
	Bias int
}

func (s *Day1ArithmeticService) Add(args day1Args, reply *int) error {
	*reply = args.A + args.B + s.Bias
	return nil
}

func (s *Day1ArithmeticService) Multiply(args day1Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (s *Day1ArithmeticService) Populate(args day1Args, reply *day1Reply) error {
	reply.Value = args.A + args.B
	reply.Label = fmt.Sprintf("%d+%d", args.A, args.B)
	return nil
}

func (s *Day1ArithmeticService) Fail(_ day1Args, _ *int) error {
	return errors.New("day1 business failure")
}

type day1TextArgs struct {
	Value string
}

type Day1TextService struct{}

func (s *Day1TextService) Upper(args day1TextArgs, reply *string) error {
	*reply = strings.ToUpper(args.Value)
	return nil
}

// Day1SignatureService deliberately mixes valid and invalid RPC signatures.
type Day1SignatureService struct{}

func (s *Day1SignatureService) NoArguments() error { return nil }

func (s *Day1SignatureService) TooManyArguments(_ day1Args, _ *int, _ int) error {
	return nil
}

func (s *Day1SignatureService) NonErrorReturn(_ day1Args, _ *int) int { return 0 }

func (s *Day1SignatureService) ValueReply(_ day1Args, _ int) error { return nil }

func (s *Day1SignatureService) Valid(args day1Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

func newDay1Client(t *testing.T, services ...interface{}) *Client {
	t.Helper()
	return newDay1ClientWithOption(t, DefaultOption, services...)
}

func newDay1ClientWithOption(t *testing.T, option *Option, services ...interface{}) *Client {
	t.Helper()

	server := NewServer()
	for _, service := range services {
		if err := server.Register(service); err != nil {
			t.Fatalf("register %T: %v", service, err)
		}
	}

	serverConn, clientConn := net.Pipe()
	server.wg.Add(1)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.handleConn(serverConn)
	}()

	client := NewClient(clientConn, option)
	t.Cleanup(func() {
		client.Close()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("server connection handler: %v", err)
			}
		case <-time.After(time.Second):
			serverConn.Close()
			t.Error("server connection handler did not stop")
		}
		serverConn.Close()
	})
	return client
}

func registerWithoutPanic(server *Server, service interface{}) (err error, panicValue interface{}) {
	defer func() {
		panicValue = recover()
	}()
	err = server.Register(service)
	return err, nil
}

func reqWithin(t *testing.T, client *Client, service, method string, args, reply interface{}) error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Req(service, method, args, reply)
	}()
	select {
	case err := <-errCh:
		return err
	case <-time.After(day1TestTimeout):
		t.Fatalf("RPC %s.%s exceeded %s", service, method, day1TestTimeout)
		return nil
	}
}

func TestDay1ServiceRegistration(t *testing.T) {
	t.Run("two services and multiple methods", func(t *testing.T) {
		server := NewServer()
		if err := server.Register(&Day1ArithmeticService{}); err != nil {
			t.Fatal(err)
		}
		if err := server.Register(&Day1TextService{}); err != nil {
			t.Fatal(err)
		}

		arithmetic, ok := server.serviceMap["Day1ArithmeticService"]
		if !ok {
			t.Fatal("Day1ArithmeticService was not registered")
		}
		if _, ok := server.serviceMap["Day1TextService"]; !ok {
			t.Fatal("Day1TextService was not registered")
		}
		for _, name := range []string{"Add", "Multiply", "Populate", "Fail"} {
			if _, ok := arithmetic.Methods[name]; !ok {
				t.Errorf("method %s was not registered", name)
			}
		}
	})

	t.Run("receiver is bound to registered instance", func(t *testing.T) {
		server := NewServer()
		receiver := &Day1ArithmeticService{Bias: 17}
		if err := server.Register(receiver); err != nil {
			t.Fatal(err)
		}
		registered := server.serviceMap["Day1ArithmeticService"]
		if got := registered.rcvr.Interface(); got != receiver {
			t.Fatalf("registered receiver = %p, want %p", got, receiver)
		}
	})

	t.Run("invalid methods are filtered", func(t *testing.T) {
		server := NewServer()
		if err := server.Register(&Day1SignatureService{}); err != nil {
			t.Fatal(err)
		}
		methods := server.serviceMap["Day1SignatureService"].Methods
		if _, ok := methods["Valid"]; !ok {
			t.Error("valid method was not registered")
		}
		for _, name := range []string{"NoArguments", "TooManyArguments", "NonErrorReturn", "ValueReply"} {
			if _, ok := methods[name]; ok {
				t.Errorf("invalid method %s was registered", name)
			}
		}
	})

	t.Run("duplicate registration is rejected", func(t *testing.T) {
		server := NewServer()
		first := &Day1ArithmeticService{Bias: 1}
		second := &Day1ArithmeticService{Bias: 2}
		if err := server.Register(first); err != nil {
			t.Fatal(err)
		}
		if err := server.Register(second); err == nil {
			t.Fatal("duplicate registration succeeded, want an error")
		}

		registered := server.serviceMap["Day1ArithmeticService"]
		if got := registered.rcvr.Interface(); got != first {
			t.Fatalf("receiver after duplicate registration = %p, want original %p", got, first)
		}
		method, ok := registered.Methods["Add"]
		if !ok {
			t.Fatal("original service lost its Add method")
		}
		var reply int
		results := method.Func.Call([]reflect.Value{
			registered.rcvr,
			reflect.ValueOf(day1Args{A: 1, B: 2}),
			reflect.ValueOf(&reply),
		})
		if err, _ := results[0].Interface().(error); err != nil {
			t.Fatalf("call original service after duplicate rejection: %v", err)
		}
		if reply != 4 {
			t.Fatalf("original service reply = %d, want 4", reply)
		}
	})
}

func TestDay1MethodSignatureValidation(t *testing.T) {
	server := NewServer()
	if err := server.Register(&Day1SignatureService{}); err != nil {
		t.Fatal(err)
	}
	methods := server.serviceMap["Day1SignatureService"].Methods

	t.Run("argument count", func(t *testing.T) {
		for _, name := range []string{"NoArguments", "TooManyArguments"} {
			if _, ok := methods[name]; ok {
				t.Errorf("method %s with invalid argument count was registered", name)
			}
		}
	})

	t.Run("return value must be error", func(t *testing.T) {
		if _, ok := methods["NonErrorReturn"]; ok {
			t.Error("method whose return value is not error was registered")
		}
	})

	t.Run("reply must be pointer", func(t *testing.T) {
		if _, ok := methods["ValueReply"]; ok {
			t.Error("method whose reply is not a pointer was registered")
		}
	})

	t.Run("valid method registers and calls", func(t *testing.T) {
		if _, ok := methods["Valid"]; !ok {
			t.Fatal("valid method was not registered")
		}
		client := newDay1Client(t, &Day1SignatureService{})
		var reply int
		if err := reqWithin(t, client, "Day1SignatureService", "Valid", day1Args{A: 20, B: 22}, &reply); err != nil {
			t.Fatalf("valid RPC method call: %v", err)
		}
		if reply != 42 {
			t.Fatalf("valid method reply = %d, want 42", reply)
		}
	})
}

func TestDay1RPCCorrectness(t *testing.T) {
	t.Run("ten sequential calls on one client", func(t *testing.T) {
		client := newDay1Client(t, &Day1ArithmeticService{})
		for i := 0; i < 10; i++ {
			var reply int
			if err := reqWithin(t, client, "Day1ArithmeticService", "Add", day1Args{A: i, B: i * 2}, &reply); err != nil {
				t.Fatalf("call %d: %v", i, err)
			}
			if want := i * 3; reply != want {
				t.Fatalf("call %d reply = %d, want %d", i, reply, want)
			}
		}
	})

	t.Run("two services", func(t *testing.T) {
		client := newDay1Client(t, &Day1ArithmeticService{}, &Day1TextService{})
		var sum int
		if err := reqWithin(t, client, "Day1ArithmeticService", "Add", day1Args{A: 19, B: 23}, &sum); err != nil {
			t.Fatal(err)
		}
		var upper string
		if err := reqWithin(t, client, "Day1TextService", "Upper", day1TextArgs{Value: "rpc"}, &upper); err != nil {
			t.Fatal(err)
		}
		if sum != 42 || upper != "RPC" {
			t.Fatalf("two-service replies = (%d, %q), want (42, %q)", sum, upper, "RPC")
		}
	})

	t.Run("reply object is populated", func(t *testing.T) {
		client := newDay1Client(t, &Day1ArithmeticService{})
		var reply day1Reply
		if err := reqWithin(t, client, "Day1ArithmeticService", "Populate", day1Args{A: 7, B: 8}, &reply); err != nil {
			t.Fatal(err)
		}
		want := day1Reply{Value: 15, Label: "7+8"}
		if reply != want {
			t.Fatalf("reply = %+v, want %+v", reply, want)
		}
	})
}

func TestDay1ConcurrentCallsDoNotMismatchResponses(t *testing.T) {
	client := newDay1Client(t, &Day1ArithmeticService{})

	type outcome struct {
		request int
		reply   int
		err     error
	}
	outcomes := make(chan outcome, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var reply int
			err := client.Req(
				"Day1ArithmeticService",
				"Add",
				day1Args{A: i, B: i * 10},
				&reply,
			)
			outcomes <- outcome{request: i, reply: reply, err: err}
		}(i)
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(day1TestTimeout):
		t.Fatalf("100 concurrent RPCs exceeded %s", day1TestTimeout)
	}
	close(outcomes)

	for got := range outcomes {
		if got.err != nil {
			t.Errorf("request %d: %v", got.request, got.err)
			continue
		}
		if want := got.request * 11; got.reply != want {
			t.Errorf("request %d reply = %d, want %d", got.request, got.reply, want)
		}
	}
}

func TestDay1ErrorPropagation(t *testing.T) {
	t.Run("service does not exist", func(t *testing.T) {
		client := newDay1Client(t, &Day1ArithmeticService{})
		var reply int
		err := reqWithin(t, client, "MissingService", "Add", day1Args{A: 1, B: 2}, &reply)
		if err == nil || !strings.Contains(err.Error(), "service MissingService not found") {
			t.Fatalf("error = %v, want missing-service error", err)
		}
	})

	t.Run("method does not exist", func(t *testing.T) {
		client := newDay1Client(t, &Day1ArithmeticService{})
		var reply int
		err := reqWithin(t, client, "Day1ArithmeticService", "MissingMethod", day1Args{A: 1, B: 2}, &reply)
		if err == nil || !strings.Contains(err.Error(), "method MissingMethod not found") {
			t.Fatalf("error = %v, want missing-method error", err)
		}
	})

	t.Run("business method error", func(t *testing.T) {
		client := newDay1Client(t, &Day1ArithmeticService{})
		var reply int
		err := reqWithin(t, client, "Day1ArithmeticService", "Fail", day1Args{}, &reply)
		if err == nil || err.Error() != "day1 business failure" {
			t.Fatalf("error = %v, want business error", err)
		}
	})

	t.Run("failed call does not affect next call", func(t *testing.T) {
		client := newDay1Client(t, &Day1ArithmeticService{})
		var ignored int
		if err := reqWithin(t, client, "MissingService", "Add", day1Args{}, &ignored); err == nil {
			t.Fatal("missing service unexpectedly succeeded")
		}
		var reply int
		if err := reqWithin(t, client, "Day1ArithmeticService", "Add", day1Args{A: 20, B: 22}, &reply); err != nil {
			t.Fatalf("normal call after error: %v", err)
		}
		if reply != 42 {
			t.Fatalf("normal call reply = %d, want 42", reply)
		}
	})
}

func TestDay1GobCodecEndToEnd(t *testing.T) {
	option := &Option{
		MagicNumber: DefaultOption.MagicNumber,
		CodecType:   Codec.GobType,
	}
	client := newDay1ClientWithOption(t, option, &Day1ArithmeticService{})

	var reply day1Reply
	err := reqWithin(
		t,
		client,
		"Day1ArithmeticService",
		"Populate",
		day1Args{A: 20, B: 22},
		&reply,
	)
	if err != nil {
		t.Fatalf("Gob RPC: %v", err)
	}
	want := day1Reply{Value: 42, Label: "20+22"}
	if reply != want {
		t.Fatalf("Gob reply = %+v, want %+v", reply, want)
	}
}

func TestDay1RegisterBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		service interface{}
	}{
		{name: "nil", service: nil},
		{name: "non-pointer receiver", service: Day1ArithmeticService{}},
		{name: "anonymous receiver", service: &struct{}{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			err, panicValue := registerWithoutPanic(server, test.service)
			if panicValue != nil {
				t.Fatalf("Register(%T) panicked: %v", test.service, panicValue)
			}
			if err == nil {
				t.Fatalf("Register(%T) succeeded, want invalid receiver error", test.service)
			}
			if len(server.serviceMap) != 0 {
				t.Fatalf("invalid receiver changed service map: %+v", server.serviceMap)
			}
		})
	}

	t.Run("non-pointer reply", func(t *testing.T) {
		server := NewServer()
		err, panicValue := registerWithoutPanic(server, &Day1SignatureService{})
		if panicValue != nil {
			t.Fatalf("Register service with invalid reply panicked: %v", panicValue)
		}
		if err != nil {
			t.Fatalf("Register valid service receiver: %v", err)
		}
		if _, ok := server.serviceMap["Day1SignatureService"].Methods["ValueReply"]; ok {
			t.Fatal("method with non-pointer reply was registered")
		}
	})
}

func TestDay1TCPLifecycle(t *testing.T) {
	server := NewServer()
	if err := server.Register(&Day1ArithmeticService{}); err != nil {
		t.Fatalf("register service: %v", err)
	}
	if err := server.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("listen: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve()
	}()

	conn, err := net.DialTimeout("tcp", server.listener.Addr().String(), time.Second)
	if err != nil {
		server.Close()
		t.Fatalf("dial server: %v", err)
	}
	client := NewClient(conn, DefaultOption)
	closed := false
	t.Cleanup(func() {
		if !closed {
			client.Close()
			server.Close()
		}
	})

	var reply int
	call, err := client.Call("Day1ArithmeticService", "Add", day1Args{A: 19, B: 23}, &reply)
	if err != nil {
		t.Fatalf("start TCP RPC: %v", err)
	}
	select {
	case <-call.Done:
		if call.err != nil {
			t.Fatalf("TCP RPC: %v", call.err)
		}
	case <-time.After(day1TestTimeout):
		t.Fatalf("TCP RPC exceeded %s", day1TestTimeout)
	}
	if reply != 42 {
		t.Fatalf("TCP reply = %d, want 42", reply)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("close server: %v", err)
	}
	closed = true
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(day1TestTimeout):
		t.Fatalf("Serve did not return after Close within %s", day1TestTimeout)
	}
}
