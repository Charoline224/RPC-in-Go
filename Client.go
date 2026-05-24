package main

import (
	"RPCinGo/Codec"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// 请求：
type Call struct {
	num           uint64
	ServiceMethod string
	args          interface{}
	resp          interface{}
	Done          chan *Call
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

// 同步
func (cli *Client) SyncReq(sm string, args interface{}, res interface{}) error {
	call := cli.AsynReq(sm, args, res)
	c := <-call.Done
	return c.err
}

// 异步
func (cli *Client) AsynReq(sm string, args interface{}, res interface{}) *Call {
	//构造请求
	var call Call
	call.Done = make(chan *Call, 1)
	call.ServiceMethod = sm
	call.args = args
	call.resp = res
	cli.Send(&call)
	return &call
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
		call.Done <- call
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
	err := cli.codec.Write(call.args, &header)
	if err != nil {
		call.err = err
		call.Done <- call
		delete(cli.calls, call.num)
		cli.err = err
		return err
	}
	return nil
}
func (cli *Client) Receive() error {
	for {
		var header Codec.Header
		err := cli.codec.ReadHeader(&header)
		if err != nil {
			cli.err = err
			return err
		}
		num := header.Num
		call := cli.calls[num]
		if call == nil {
			cli.codec.ReadBody(nil)
			continue
		}
		parseError := header.Error
		if parseError != "" {
			call.err = errors.New(parseError)
			cli.codec.ReadBody(nil)
			call.Done <- call
			continue
		}
		err = cli.codec.ReadBody(call.resp)
		if err != nil {
			call.err = err
			call.Done <- call
			continue
		}
		call.Done <- call
	}
}
