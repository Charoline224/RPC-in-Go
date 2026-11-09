package main

import (
	"RPCinGo/Codec"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestRPCJSONRoundTrip(t *testing.T) {
	server := NewServer()
	if err := server.Register(&MathService{}); err != nil {
		t.Fatalf("register service: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	server.wg.Add(1)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.handleConn(serverConn)
	}()

	client := NewClient(clientConn, DefaultOption)
	t.Cleanup(func() {
		client.Close()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("handle connection: %v", err)
			}
		case <-time.After(time.Second):
			serverConn.Close()
			t.Error("server connection handler did not stop")
		}
		serverConn.Close()
	})

	var got int
	if err := client.Req("MathService", "Add", Args{A: 1, B: 2}, &got); err != nil {
		t.Fatalf("call Add: %v", err)
	}
	if got != 3 {
		t.Fatalf("Add result = %d, want 3", got)
	}
}

func TestUnknownMethodConsumesRequestBody(t *testing.T) {
	server := NewServer()
	if err := server.Register(&MathService{}); err != nil {
		t.Fatalf("register service: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	server.wg.Add(1)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.handleConn(serverConn)
	}()
	t.Cleanup(func() {
		clientConn.Close()
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			serverConn.Close()
			t.Error("server connection handler did not stop")
		}
		serverConn.Close()
	})

	if err := clientConn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set connection deadline: %v", err)
	}
	if err := json.NewEncoder(clientConn).Encode(DefaultOption); err != nil {
		t.Fatalf("send option: %v", err)
	}
	codec := Codec.NewJsonCodec(clientConn)

	firstWrite := make(chan error, 1)
	go func() {
		firstWrite <- codec.Write(Args{A: 1, B: 2}, &Codec.Header{
			Num:     1,
			Service: "MathService",
			Method:  "Missing",
		})
	}()
	var first Codec.Header
	if err := codec.ReadHeader(&first); err != nil {
		t.Fatalf("read unknown-method response header: %v", err)
	}
	if err := codec.ReadBody(nil); err != nil {
		t.Fatalf("read unknown-method response body: %v", err)
	}
	if err := <-firstWrite; err != nil {
		t.Fatalf("write unknown-method request: %v", err)
	}
	if first.Num != 1 || first.Error == "" {
		t.Fatalf("unknown-method response = %+v, want num 1 with an error", first)
	}

	secondWrite := make(chan error, 1)
	go func() {
		secondWrite <- codec.Write(Args{A: 3, B: 4}, &Codec.Header{
			Num:     2,
			Service: "MathService",
			Method:  "Add",
		})
	}()
	var second Codec.Header
	if err := codec.ReadHeader(&second); err != nil {
		t.Fatalf("read Add response header: %v", err)
	}
	var got int
	if err := codec.ReadBody(&got); err != nil {
		t.Fatalf("read Add response body: %v", err)
	}
	if err := <-secondWrite; err != nil {
		t.Fatalf("write Add request: %v", err)
	}
	if second.Num != 2 || second.Error != "" || got != 7 {
		t.Fatalf("Add response = (%+v, %d), want num 2 and result 7", second, got)
	}
}
