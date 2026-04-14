package cproto

import (
	"testing"

	"github.com/golang/snappy"
	"github.com/restream/reindexer/v5/bindings"
	"github.com/stretchr/testify/require"
)

func BenchmarkRPCEncoderInt32ArrArg(b *testing.B) {
	values := make([]int32, 2048)
	for i := range values {
		values[i] = int32(i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc := newRPCEncoder(cmdSelect, uint32(i), false, false)
		enc.int32ArrArg(values)
		_ = enc.bytes()
		enc.ser.Close()
	}
}

func BenchmarkRPCEncoderSnappyBytes(b *testing.B) {
	payload := make([]byte, 64<<10)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for i := 0; b.Loop(); i++ {
		enc := newRPCEncoder(cmdSelect, uint32(i), true, false)
		enc.bytesArg(payload)
		_ = enc.bytes()
		enc.ser.Close()
	}
}

func BenchmarkRPCDecoderIntfArgs(b *testing.B) {
	enc := newRPCEncoder(cmdSelect, 1, false, false)
	enc.intArg(42)
	enc.boolArg(true)
	enc.stringArg("decode-bench")
	wire := append([]byte(nil), enc.bytes()...)
	enc.ser.Close()

	// Build payload compatible with rpcDecoder parsing in NetBuffer.parseArgs:
	// errCode=0, errString="", argsCount=3, then args in wire format.
	reply := make([]byte, 0, len(wire))
	ser := newRPCEncoder(cmdSelect, 1, false, false)
	ser.ser.Reset()
	ser.ser.PutVarUInt(0)
	ser.ser.PutVString("")
	ser.ser.PutVarUInt(3)
	ser.ser.PutVarUInt(uint64(bindings.ValueInt))
	ser.ser.PutVarInt(42)
	ser.ser.PutVarUInt(uint64(bindings.ValueBool))
	ser.ser.PutVarUInt(1)
	ser.ser.PutVarUInt(uint64(bindings.ValueString))
	ser.ser.PutVString("decode-bench")
	reply = append(reply, ser.ser.Bytes()...)
	ser.ser.Close()

	b.ReportAllocs()
	b.SetBytes(int64(len(reply)))

	for b.Loop() {
		dec := newRPCDecoder(reply)
		err := dec.errCode()
		require.NoError(b, err)
		cnt := dec.argsCount()
		for j := 0; j < cnt; j++ {
			_ = dec.intfArg()
		}
	}
}

func BenchmarkNetBufferDecompressReuse(b *testing.B) {
	raw := make([]byte, 256<<10)
	for i := range raw {
		raw[i] = byte(i)
	}
	compressed := snappy.Encode(nil, raw)

	nb := &NetBuffer{}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))

	for b.Loop() {
		nb.buf = compressed
		err := nb.decompress()
		require.NoError(b, err)
	}
}
