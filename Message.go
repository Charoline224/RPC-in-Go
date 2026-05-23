package rpcingo

import "RPCinGo/Codec"

type Option struct {
	MagicNumber int        // 暗号，验证是不是自己的RPC请求
	CodecType   Codec.Type // 编码类型
}

// 默认选择，魔数用于比对确认是发给这个server的
var DefaultOption = &Option{
	MagicNumber: 0x3bef5c, // 随便一个魔数
	CodecType:   Codec.GobType,
}
