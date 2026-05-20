package Codec

import (
	"encoding/json"
	"io"
	"log"
)

type JsonCodec struct {
	base *BaseCodec
	enc  *json.Encoder
	dec  *json.Decoder
}

func NewJsonCodec(conn io.ReadWriteCloser) Codec {
	var j JsonCodec
	j.base = NewBaseCodec(conn)
	j.enc = json.NewEncoder(j.base.buf)
	j.dec = json.NewDecoder(j.base.conn)
	return &j
}

// 实现所有codec功能自动接上接口
func (c *JsonCodec) Close() error {
	return c.base.conn.Close()
}
func (c *JsonCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}
func (c *JsonCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}
func (c *JsonCodec) Write(body interface{}, h *Header) error {
	defer func() {
		err := c.base.buf.Flush()
		if err != nil {
			c.Close()
		}
	}()
	err := c.enc.Encode(h)
	if err != nil {
		log.Println("rpc codec: json error encoding header:", err)
		return err
	}
	err = c.enc.Encode(body)
	if err != nil {
		log.Println("rpc codec: json error encoding body:", err)
		return err
	}
	return nil
}
