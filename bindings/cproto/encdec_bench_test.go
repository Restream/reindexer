package cproto

import (
	"context"
	"testing"
	"unsafe"

	"github.com/golang/snappy"
	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
	"github.com/stretchr/testify/require"
)

var str2cSink reindexerStringView

type reindexerStringView struct {
	p unsafe.Pointer
	n int
}

func str2cByStringData(str string) reindexerStringView {
	return reindexerStringView{p: unsafe.Pointer(unsafe.StringData(str)), n: len(str)}
}

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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc := newRPCEncoder(cmdSelect, uint32(i), true, false)
		enc.bytesArg(payload)
		_ = enc.bytes()
		enc.ser.Close()
	}
}

func BenchmarkRPCEncoderStartArgsChunck(b *testing.B) {
	enc := newRPCEncoder(cmdSelect, 1, false, false)
	defer enc.ser.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.ser.Truncate(cprotoHdrLen)
		enc.startArgsChunck()
	}
}

func BenchmarkRPCEncoderUpdate(b *testing.B) {
	enc := newRPCEncoder(cmdSelect, 1, false, false)
	defer enc.ser.Close()

	enc.ser.Truncate(cprotoHdrLen)
	enc.startArgsChunck()
	enc.ser.PutVarUInt(uint64(bindings.ValueInt))
	enc.ser.PutVarInt(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.update()
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nb.buf = compressed
		err := nb.decompress()
		require.NoError(b, err)
	}
}

func BenchmarkStr2CByStringData(b *testing.B) {
	benchStr2CConversion(b, str2cByStringData)
}

func BenchmarkNetBufferParseArgs(b *testing.B) {
	reply := buildParseArgsReplyPayload()
	nb := &NetBuffer{
		buf:  reply,
		args: make([]any, 0, 8),
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(reply)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := nb.parseArgs()
		require.NoError(b, err)
		if len(nb.args) != 5 {
			b.Fatalf("unexpected args len: %d", len(nb.args))
		}
	}
}

func BenchmarkNetBufferParseArgsTimeout(b *testing.B) {
	reply := buildParseArgsErrorPayload(bindings.ErrTimeout, "timeout")
	nb := &NetBuffer{
		buf: reply,
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(reply)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := nb.parseArgs()
		if err != context.DeadlineExceeded {
			b.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	}
}

func benchStr2CConversion(b *testing.B, fn func(string) reindexerStringView) {
	inputs := []string{
		"",
		"a",
		"short-value",
		"reindexer-cproto-string-conversion-benchmark",
		string(make([]byte, 128)),
		string(make([]byte, 4096)),
	}
	dyn128 := make([]byte, 128)
	for i := range dyn128 {
		dyn128[i] = byte('a' + i%26)
	}
	dyn4k := make([]byte, 4096)
	for i := range dyn4k {
		dyn4k[i] = byte(i)
	}
	inputs[4] = string(dyn128)
	inputs[5] = string(dyn4k)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str2cSink = fn(inputs[i%len(inputs)])
	}
}

func buildParseArgsReplyPayload() []byte {
	ser := cjson.NewPoolSerializer()
	defer ser.Close()

	ser.PutVarUInt(0)
	ser.PutVString("")
	ser.PutVarUInt(5)

	ser.PutVarUInt(uint64(bindings.ValueInt))
	ser.PutVarInt(42)

	ser.PutVarUInt(uint64(bindings.ValueBool))
	ser.PutVarUInt(1)

	ser.PutVarUInt(uint64(bindings.ValueString))
	ser.PutVString("parse-args-bench")

	ser.PutVarUInt(uint64(bindings.ValueInt64))
	ser.PutVarInt(123456789)

	ser.PutVarUInt(uint64(bindings.ValueDouble))
	ser.PutDouble(3.1415926535)

	return append([]byte(nil), ser.Bytes()...)
}

func buildParseArgsErrorPayload(code int, msg string) []byte {
	ser := cjson.NewPoolSerializer()
	defer ser.Close()

	ser.PutVarUInt(uint64(code))
	ser.PutVString(msg)

	return append([]byte(nil), ser.Bytes()...)
}
