package Codec

import (
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	base *BaseCodec
	enc  *gob.Encoder
	dec  *gob.Decoder
}

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	var g GobCodec
	g.base = NewBaseCodec(conn)
	g.enc = gob.NewEncoder(g.base.buf)
	g.dec = gob.NewDecoder(g.base.conn)
	return &g
}

// 实现所有codec功能自动接上接口
func (c *GobCodec) Close() error {
	return c.base.conn.Close()
}
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}
func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}
func (c *GobCodec) Write(body interface{}, h *Header) error {
	defer func() {
		err := c.base.buf.Flush()
		if err != nil {
			c.Close()
		}
	}()
	err := c.enc.Encode(h)
	if err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	err = c.enc.Encode(body)
	if err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}
