package framing_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	framing "RPCinGo/transport"
)

type testArgs struct {
	A    int      `json:"a"`
	Text string   `json:"text"`
	Tags []string `json:"tags"`
}

func TestJSONFrameRoundTrip(t *testing.T) {
	want := testArgs{
		A:    42,
		Text: "RPC 完整链路",
		Tags: []string{"json", "frame"},
	}

	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}

	framer := framing.Framer{}
	frame, err := framer.EncodeMessage(payload)
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}

	decodedPayload, err := framer.DecodeMessage(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if !bytes.Equal(decodedPayload, payload) {
		t.Fatalf("payload changed after framing: got %q, want %q", decodedPayload, payload)
	}

	var got testArgs
	if err := json.Unmarshal(decodedPayload, &got); err != nil {
		t.Fatalf("unmarshal args: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip result = %#v, want %#v", got, want)
	}
}
