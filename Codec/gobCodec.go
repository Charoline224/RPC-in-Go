package Codec

import (
	"bytes"
	"encoding/gob"
	"errors"
	"reflect"
)

type GobCodec struct{}

func validateDestination(v any) error {
	if v == nil {
		return errors.New("decode destination is nil")
	}

	rv := reflect.ValueOf(v)

	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("decode destination must be a non-nil pointer")
	}

	return nil
}

func (c *GobCodec) Marshel(v any) ([]byte, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *GobCodec) Unmarshal(data []byte, v any) error {
	if err := validateDestination(v); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// 实现所有codec功能自动接上接口
// func (c *GobCodec) Close() error {
// 	return c.base.conn.Close()
// }
// func (c *GobCodec) ReadHeader(h *Header) error {
// 	return c.dec.Decode(h)
// }
// func (c *GobCodec) ReadBody(body interface{}) error {
// 	return c.dec.Decode(body)
// }
// func (c *GobCodec) Write(body interface{}, h *Header) (err error) {
// 	c.base.lock.Lock()
// 	defer c.base.lock.Unlock()
// 	defer func() {
// 		//把buf里面的缓冲字节写到conn
// 		flushErr := c.base.buf.Flush()
// 		if flushErr != nil {
// 			c.Close()
// 			if err == nil {
// 				err = flushErr
// 			}
// 		}
// 	}()
// 	//编码head，写到buf
// 	err = c.enc.Encode(h)
// 	if err != nil {
// 		log.Println("rpc codec: gob error encoding header:", err)
// 		return err
// 	}
// 	//编码body，写到buf
// 	err = c.enc.Encode(body)
// 	if err != nil {
// 		log.Println("rpc codec: gob error encoding body:", err)
// 		return err
// 	}
// 	return nil
// }
