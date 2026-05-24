package main

import (
	"fmt"
	"net"
	"time"
)

type MathService struct{}
type Args struct{ A, B int }

func (s *MathService) Add(args Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

func main() {
	//建立服务端
	server := NewServer()
	//注册服务
	var m MathService
	server.Register(&m)
	//开始监听
	go server.Listen(":8080")
	//建立客户端
	time.Sleep(time.Second)
	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		return
	}
	client := NewClient(conn, DefaultOption)
	//客户端发请求
	var resp int
	err = client.Req("Add", Args{A: 1, B: 2}, &resp)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp)
	client.Close()
}
