package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "RPCinGo/internal/exampletest"
	framing "RPCinGo/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type calculatorService struct{}

func (*calculatorService) Add(_ context.Context, req *pb.AddRequest) (*pb.AddResponse, error) {
	return &pb.AddResponse{Result: req.GetA() + req.GetB()}, nil
}

func TestServeHTTPUnarySuccess(t *testing.T) {
	server := NewServer()
	if err := server.Register("calculator.Calculator", &calculatorService{}); err != nil {
		t.Fatalf("register service: %v", err)
	}

	payload, err := proto.Marshal(&pb.AddRequest{A: 20, B: 22})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	frame, err := (framing.Framer{}).EncodeMessage(payload)
	if err != nil {
		t.Fatalf("frame request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/calculator.Calculator/Add", bytes.NewReader(frame))
	req.ProtoMajor = 2
	req.ProtoMinor = 0
	req.Header.Set("Content-Type", "application/grpc")
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	result := recorder.Result()
	if result.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want %d", result.StatusCode, http.StatusOK)
	}
	if got := result.Trailer.Get("Grpc-Status"); got != "0" {
		t.Fatalf("Grpc-Status trailer = %q, want %q", got, "0")
	}

	responsePayload, err := (framing.Framer{}).DecodeMessage(result.Body)
	if err != nil {
		t.Fatalf("decode response frame: %v", err)
	}
	var response pb.AddResponse
	if err := proto.Unmarshal(responsePayload, &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got, want := response.GetResult(), int32(42); got != want {
		t.Fatalf("result = %d, want %d", got, want)
	}
}

func TestServerUnaryCallOverHTTP2(t *testing.T) {
	server := NewServer()
	if err := server.Register("calculator.Calculator", &calculatorService{}); err != nil {
		t.Fatalf("register service: %v", err)
	}
	if err := server.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("listen: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve()
	}()

	conn, err := grpc.NewClient(
		server.listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.listener.Close()
		t.Fatalf("create gRPC client: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		server.listener.Close()
		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("serve stopped with unexpected error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("server did not stop after listener was closed")
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := pb.NewCalculatorClient(conn).Add(ctx, &pb.AddRequest{A: 20, B: 22})
	if err != nil {
		t.Fatalf("Add RPC: %v", err)
	}
	if got, want := response.GetResult(), int32(42); got != want {
		t.Fatalf("result = %d, want %d", got, want)
	}
}
