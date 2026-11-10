package framing

import (
	"encoding/binary"
	"errors"
	"io"
)

const MaxMessageSize = 4 << 20

type Framer struct{}

func (Framer) EncodeMessage(data []byte) ([]byte, error) {
	if len(data) > MaxMessageSize {
		return nil, errors.New("message too large")
	}
	message := make([]byte, 5+len(data))
	message[0] = 0
	binary.BigEndian.PutUint32(message[1:5], uint32(len(data)))
	copy(message[5:], data)
	return message, nil
}

func (Framer) DecodeMessage(r io.Reader) ([]byte, error) {
	var prefix [5]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, err
	}
	if prefix[0] != 0 {
		return nil, errors.New("暂时不支持压缩")
	}
	length := binary.BigEndian.Uint32(prefix[1:5])
	if length > MaxMessageSize {
		return nil, errors.New("消息超长")
	}
	message := make([]byte, int(length))
	if _, err := io.ReadFull(r, message); err != nil {
		return nil, err
	}
	return message, nil
}
