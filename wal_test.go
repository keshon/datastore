package datastore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"testing"
)

func TestFrameEncodingPreservesPayloadBytes(t *testing.T) {
	for _, frame := range []*walFrame{
		{Seq: 1, TS: 2, Counters: map[string]uint64{"id": 4}},
		{
			Seq:  12,
			Ops:  []walOp{{Coll: "<records>", Key: "ключ\n", Op: opPut, Val: json.RawMessage(` {"nested":[1,"<>&"]} `)}},
			Meta: map[string]string{"actor": "a\"b"},
		},
	} {
		want, err := json.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := encodeFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encoded[frameHeaderSize:], want) {
			t.Fatal("frame payload changed from json.Marshal encoding")
		}
		length := binary.LittleEndian.Uint32(encoded[:4])
		checksum := binary.LittleEndian.Uint32(encoded[4:8])
		if int(length) != len(want) || checksum != crc32.Checksum(want, crcTable) {
			t.Fatal("incorrect frame length or checksum")
		}
	}
}
