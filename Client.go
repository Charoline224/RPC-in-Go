package main

import (
	"RPCinGo/Codec"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// 请求：
// call持有管道是为了定向通知结果
type Call struct {
	num           uint64
	ServiceMethod string
	args          interface{}
	resp          interface{}
	Done          chan struct{}
	err           error
}

//不需要resp结构体，直接返回函数调用结果就行了
// type Resp struct{
// }

// 发出请求的对象（包含一系列请求）
type Client struct {
	//通信
	conn  io.ReadWriteCloser //连接
	codec Codec.Codec        //编码(使用接口才能适用各种流)
	//并发请求管理
	cnt   uint64 //（请求编号原子自增）
	calls map[uint64](*Call)
	lock  sync.Mutex
	//自身状态
	err   error
	close bool
}

func NewClient(conn io.ReadWriteCloser, o *Option) *Client {
	json.NewEncoder(conn).Encode(o)
	var c Client
	t := o.CodecType
	c.conn = conn
	c.codec = Codec.NewCodecFuncMap[t](conn)
	c.cnt = 0
	c.calls = make(map[uint64]*Call)
	c.err = nil
	c.close = false
	go c.Receive()
	return &c
}

// 同步
func (cli *Client) Req(name string, args any, res interface{}) error {
	call, err := cli.Call(name, args, res)
	if err != nil {
		return err
	}
	//阻塞在此处，等待管道返回已经done的call
	<-call.Done
	cli.lock.Lock()
	delete(cli.calls, call.num)
	cli.lock.Unlock()
	fmt.Println("call done, err:", call.err)
	return call.err
}

// 异步调用
func (cli *Client) Call(sm string, args interface{}, res interface{}) (*Call, error) {
	//构造请求
	var call Call
	call.Done = make(chan struct{})
	call.ServiceMethod = sm
	call.args = args
	call.resp = res
	err := cli.Send(&call)
	if err != nil {
		return nil, err
	}
	return &call, nil
}

// 关闭client
func (cli *Client) Close() error {
	cli.lock.Lock()
	defer cli.lock.Unlock()
	if cli.close {
		return cli.err
	}
	err := cli.conn.Close()
	if err != nil {
		return err
	}
	cli.close = true
	cli.err = errors.New("client is closed")
	//call未处理完，但是关闭client了,剩下的call要全部终结
	for _, call := range cli.calls {
		call.err = cli.err
		call.Done <- struct{}{}
	}

	return nil
}
func (cli *Client) Send(call *Call) error {
	if call == nil {
		return errors.New("call doesn't exist")
	}
	//写header和body（参数struct）
	var header Codec.Header
	header.Num = atomic.AddUint64(&cli.cnt, 1)
	header.ServiceMethod = call.ServiceMethod
	call.num = header.Num
	cli.lock.Lock()
	defer cli.lock.Unlock()
	cli.calls[call.num] = call //加入请求队列
	//编码message
	err := cli.codec.Write(call.args, &header)
	if err != nil {
		call.err = err
		call.Done <- struct{}{}
		delete(cli.calls, call.num)
		cli.err = err
		return err
	}
	fmt.Println("send request:", call.ServiceMethod, "num:", call.num)
	return nil
}
func (cli *Client) Receive() error {
	fmt.Println("client start receiving")
	for {
		var header Codec.Header
		err := cli.codec.ReadHeader(&header)
		if err != nil {
			cli.err = err
			return err
		}
		fmt.Println("receive response:", header.ServiceMethod, "num:", header.Num)
		num := header.Num
		cli.lock.Lock()
		call := cli.calls[num]
		cli.lock.Unlock()
		if call == nil {
			cli.codec.ReadBody(nil)
			continue
		}
		parseError := header.Error
		if parseError != "" {
			call.err = errors.New(parseError)
			cli.codec.ReadBody(nil)
			call.Done <- struct{}{}
			continue
		}
		err = cli.codec.ReadBody(call.resp)
		if err != nil {
			call.err = err
			call.Done <- struct{}{}
			continue
		}
		call.Done <- struct{}{}

	}
}
