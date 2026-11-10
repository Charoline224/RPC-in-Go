package Codec

import (
	"io"
	"sync"
)

// header结构体
type Header struct {
	Num     uint64
	Service string
	Method  string
	Length  int
	Error   string
}

// Codec的结构体公共部分
type BaseCodec struct {
	lock *sync.Mutex
}

func NewBaseCodec() *BaseCodec {
	var b BaseCodec
	b.lock = new(sync.Mutex)
	return &b
}

// Codec接口，实现功能
type Codec interface {
	Marshel(v any) ([]byte, error)
	Unmarshal(data []byte) (any, error)
}

// 实现根据type初始化codec工厂函数
type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

type NewCodecFunc func(io.ReadWriteCloser) Codec

var NewCodecFuncMap = make(map[Type]NewCodecFunc)

// 自动注册
func init() {
	NewCodecFuncMap[GobType] = NewGobCodec
	NewCodecFuncMap[JsonType] = NewJsonCodec
}
