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
func (c *GobCodec) Write(body interface{}, h *Header) (err error) {
	c.base.lock.Lock()
	defer c.base.lock.Unlock()
	defer func() {
		//把buf里面的缓冲字节写到conn
		flushErr := c.base.buf.Flush()
		if flushErr != nil {
			c.Close()
			if err == nil {
				err = flushErr
			}
		}
	}()
	//编码head，写到buf
	err = c.enc.Encode(h)
	if err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	//编码body，写到buf
	err = c.enc.Encode(body)
	if err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}
