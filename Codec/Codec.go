package Codec

import (
	"bufio"
	"io"
)

// header结构体
type Header struct {
	Num           uint64
	ServiceMethod string
	Length        int
	Error         string
}

// Codec的结构体公共部分
type BaseCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
}

func NewBaseCodec(conn io.ReadWriteCloser) *BaseCodec {
	var b BaseCodec
	b.buf = bufio.NewWriter(conn)
	b.conn = conn
	return &b
}

// Codec接口，实现功能
type Codec interface {
	io.Closer
	ReadHeader(header *Header) error
	//不知道body是req还是resp，所以用interface传指针
	ReadBody(body interface{}) error
	Write(body interface{}, header *Header) error
}

// 实现根据type初始化codec工厂函数
type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

type NewCodecFunc func(io.ReadWriteCloser) Codec

var NewCodecFuncMap = make(map[Type]NewCodecFunc)

func init() {
	NewCodecFuncMap[GobType] = NewGobCodec
	NewCodecFuncMap[JsonType] = NewJsonCodec
}
