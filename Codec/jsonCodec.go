package Codec

import (
	"bytes"
	"encoding/json"
)

type JsonCodec struct{}

func (c *JsonCodec) Marshel(v any) ([]byte, error) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *JsonCodec) Unmarshal(data []byte, v any) error {
	if err := validateDestination(v); err != nil {
		return err
	}
	return json.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// func NewJsonCodec(conn io.ReadWriteCloser) Codec {
// 	var j JsonCodec
// 	j.base = NewBaseCodec(conn)
// 	j.enc = json.NewEncoder(j.base.buf)
// 	j.dec = json.NewDecoder(j.base.conn)
// 	return &j
// }

// // 实现所有codec功能自动接上接口
// func (c *JsonCodec) Close() error {
// 	return c.base.conn.Close()
// }
// func (c *JsonCodec) ReadHeader(h *Header) error {
// 	fmt.Println("json codec read header")
// 	return c.dec.Decode(h)
// }
// func (c *JsonCodec) ReadBody(body interface{}) error {
// 	if body == nil {
// 		//JSON的Decode(nil)空body会报错
// 		var discard json.RawMessage
// 		return c.dec.Decode(&discard)
// 	}
// 	return c.dec.Decode(body)
// }
// func (c *JsonCodec) Write(body interface{}, h *Header) (err error) {
// 	c.base.lock.Lock()
// 	defer c.base.lock.Unlock()
// 	defer func() {
// 		flushErr := c.base.buf.Flush()
// 		if flushErr != nil {
// 			c.Close()
// 			if err == nil {
// 				err = flushErr
// 			}
// 		}
// 	}()
// 	err = c.enc.Encode(h)
// 	if err != nil {
// 		log.Println("rpc codec: json error encoding header:", err)
// 		return err
// 	}
// 	err = c.enc.Encode(body)
// 	if err != nil {
// 		log.Println("rpc codec: json error encoding body:", err)
// 		return err
// 	}
// 	return nil
// }
