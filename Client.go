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
	num     uint64
	service string
	method  string
	args    interface{}
	resp    interface{}
	Done    chan struct{}
	err     error
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
	err := json.NewEncoder(conn).Encode(o)
	var c Client
	t := o.CodecType
	c.conn = conn
	c.codec = Codec.NewCodecFuncMap[t](conn)
	c.cnt = 0
	c.calls = make(map[uint64]*Call)
	c.err = nil
	c.close = false
	if err != nil {
		c.terminate(err)
		return &c
	}
	go c.Receive()
	return &c
}

// 同步
func (cli *Client) Req(service string, method string, args any, res interface{}) error {
	call, err := cli.Call(service, method, args, res)
	if err != nil {
		return err
	}
	//阻塞在此处，等待管道返回已经done的call
	<-call.Done
	fmt.Println("call done, err:", call.err)
	return call.err
}

// 异步调用
func (cli *Client) Call(service string, method string, args interface{}, res interface{}) (*Call, error) {
	//构造请求
	var call Call
	call.Done = make(chan struct{})
	call.service = service
	call.method = method
	call.args = args
	call.resp = res
	err := cli.Send(&call)
	if err != nil {
		return nil, err
	}
	return &call, nil
}

// 正常关闭client
func (cli *Client) Close() error {
	cli.terminate(errors.New("client is closed"))
	return nil
}

// 修改状态：从待处理表移除call，即取得该 Call 的独占完成权。
func (cli *Client) takeCall(num uint64) *Call {
	call := cli.calls[num]
	delete(cli.calls, num)
	return call
}

// 只结束一个call。错误出现时候，打破死锁的设计。
// 死锁在于：send不return就req就无法进行到接收，管道堵塞；但是req管道没有接收，send的管道就堵塞，无法到达return。
func (call *Call) finish(err error) {
	call.err = err
	close(call.Done) // 不要求调用者正在接收，也允许稍后等待。
}

// 结束一个client。任何终止，可能由错误引起
func (cli *Client) terminate(err error) {
	cli.lock.Lock()
	if cli.close {
		cli.lock.Unlock()
		return
	}
	cli.close = true
	cli.err = err
	for num := range cli.calls {
		cli.takeCall(num).finish(err)
	}
	cli.lock.Unlock()
	// 唤醒正在读响应体或发送请求的 goroutine；不持状态锁做网络操作。
	cli.conn.Close()
}
func (cli *Client) Send(call *Call) error {
	if call == nil {
		return errors.New("call doesn't exist")
	}
	//写header和body（参数struct）
	var header Codec.Header
	header.Num = atomic.AddUint64(&cli.cnt, 1)
	header.Service = call.service
	header.Method = call.method
	call.num = header.Num

	cli.lock.Lock()
	//如果cli已经关闭了
	if cli.close {
		err := cli.err
		cli.lock.Unlock()
		call.finish(err)
		return err
	}
	cli.calls[call.num] = call //加入请求队列
	cli.lock.Unlock()
	//编码message
	err := cli.codec.Write(call.args, &header)
	if err != nil {
		cli.terminate(err)
		return err
	}
	return nil
}
func (cli *Client) Receive() error {
	for {
		var header Codec.Header
		err := cli.codec.ReadHeader(&header)
		if err != nil {
			cli.terminate(err)
			return err
		}
		num := header.Num
		//找到call对象 + 修改状态
		cli.lock.Lock()
		call := cli.takeCall(num)
		cli.lock.Unlock()
		if call == nil {
			if err := cli.codec.ReadBody(nil); err != nil {
				cli.terminate(err)
				return err
			}
			continue
		}
		//解析错误
		parseError := header.Error
		if parseError != "" {
			err := cli.codec.ReadBody(nil)
			call.finish(errors.New(parseError))
			if err != nil {
				cli.terminate(err)
				return err
			}
			continue
		}
		err = cli.codec.ReadBody(call.resp)
		if err != nil {
			call.finish(err)
			cli.terminate(err)
			return err
		}
		call.finish(nil)

	}
}
