package main

import (
	"RPCinGo/Codec"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func testClient(t *testing.T) (*Client, net.Conn, <-chan struct{}) {
	t.Helper()
	local, remote := net.Pipe()
	cli := &Client{conn: local, codec: Codec.NewJsonCodec(local), calls: make(map[uint64]*Call)}
	stopped := make(chan struct{})
	go func() { cli.Receive(); close(stopped) }()
	t.Cleanup(func() { cli.Close(); remote.Close() })
	return cli, remote, stopped
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("completion blocked")
	}
}

func TestAsyncCompletion(t *testing.T) {
	cli, remote, stopped := testClient(t)
	go func() {
		codec := Codec.NewJsonCodec(remote)
		for i := 0; i < 2; i++ {
			var h Codec.Header
			var args Args
			if codec.ReadHeader(&h) != nil || codec.ReadBody(&args) != nil {
				return
			}
			if i == 0 {
				h.Error = "method failed"
			}
			if codec.Write(args.A+args.B, &h) != nil {
				return
			}
		}
		remote.Close()
	}()
	var results [2]int
	first, err := cli.Call("MathService", "Add", Args{1, 2}, &results[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := cli.Call("MathService", "Add", Args{3, 4}, &results[1])
	if err != nil {
		t.Fatal(err)
	}
	// 在 Receive 退出之前不接收 Done，验证异步调用不会阻塞读取。
	waitDone(t, stopped)
	waitDone(t, first.Done)
	waitDone(t, second.Done)
	if first.err == nil || second.err != nil || results[1] != 7 {
		t.Fatalf("unexpected results: %v, %v, %v", first.err, second.err, results)
	}
	if len(cli.calls) != 0 {
		t.Fatal("completed calls retained")
	}
}

func TestDisconnectAndClose(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		cli, remote, stopped := testClient(t)
		go io.Copy(io.Discard, remote)
		var result int
		call, err := cli.Call("MathService", "Add", Args{1, 2}, &result)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				defer wg.Done()
				if disconnect {
					remote.Close()
				} else {
					cli.Close()
				}
			}()
		}
		wg.Wait()
		waitDone(t, call.Done)
		waitDone(t, stopped)
		if call.err == nil || len(cli.calls) != 0 {
			t.Fatal("pending call not failed and removed")
		}
		if _, err := cli.Call("MathService", "Add", Args{}, &result); err == nil {
			t.Fatal("closed client accepted call")
		}
	}
}

func TestSendFailure(t *testing.T) {
	local, remote := net.Pipe()
	remote.Close()
	cli := &Client{conn: local, codec: Codec.NewJsonCodec(local), calls: make(map[uint64]*Call)}
	t.Cleanup(func() { cli.Close() })
	call := &Call{service: "MathService", method: "Add", args: Args{}, Done: make(chan struct{})}
	if err := cli.Send(call); err == nil {
		t.Fatal("write failure was lost")
	}
	waitDone(t, call.Done)
	if call.err == nil || len(cli.calls) != 0 {
		t.Fatal("failed call retained")
	}
}
