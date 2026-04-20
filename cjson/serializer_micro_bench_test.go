package cjson

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

var (
	benchUint64Sink uint64
	benchBytesSink  []byte
)

func benchmarkSerializerBytesOp(b *testing.B, op func(*Serializer, []byte), headerReserve int) {
	for _, size := range []int{16, 256, 4 << 10, 64 << 10} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i)
		}

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			ser := NewSerializer(make([]byte, 0, size+headerReserve))
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				ser.Reset()
				op(&ser, payload)
			}
			benchBytesSink = ser.Bytes()
		})
	}
}

func BenchmarkSerializerWriteIntBits(b *testing.B) {
	sizes := []uintptr{1, 2, 4, 8}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			ser := NewSerializer(make([]byte, 0, b.N*int(sz)))
			b.ReportAllocs()
			b.SetBytes(int64(sz))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ser.writeIntBits(int64(i), sz)
			}
			benchBytesSink = ser.buf
		})
	}
}

func BenchmarkSerializerPutFloatVector(b *testing.B) {
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i) / 100
	}

	ser := NewSerializer(make([]byte, 0, len(vec)*4+binary.MaxVarintLen64))
	b.ReportAllocs()
	b.SetBytes(int64(len(vec) * 4))
	b.ResetTimer()
	for b.Loop() {
		ser.Reset()
		ser.PutFloatVector(vec)
	}
	benchBytesSink = ser.Bytes()
}

func BenchmarkSerializerWriteString(b *testing.B) {
	payload := "write-string-benchmark-payload-abcdefghijklmnopqrstuvwxyz"
	ser := NewSerializer(make([]byte, 0, len(payload)))
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for b.Loop() {
		ser.Reset()
		ser.WriteString(payload)
	}
	benchBytesSink = ser.Bytes()
}

func BenchmarkSerializerWriteInts(b *testing.B) {
	values := make([]int, 4096)
	for i := range values {
		if i%2 == 0 {
			values[i] = i
		} else {
			values[i] = -i
		}
	}

	ser := NewSerializer(make([]byte, 0, len(values)*8))
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for b.Loop() {
		ser.Reset()
		_, _ = ser.WriteInts(values)
	}
	benchBytesSink = ser.Bytes()
}

func BenchmarkSerializerGetDouble(b *testing.B) {
	const count = 4096
	wr := NewPoolSerializer()
	for i := range count {
		wr.PutDouble(float64(i) * math.Pi)
	}
	payload := append([]byte(nil), wr.Bytes()...)
	wr.Close()

	rd := NewSerializer(payload)
	b.ReportAllocs()
	b.SetBytes(8)

	for b.Loop() {
		if rd.pos+8 > len(rd.buf) {
			rd.pos = 0
		}
		benchUint64Sink ^= math.Float64bits(rd.GetDouble())
	}
}

func BenchmarkSerializerAppend(b *testing.B) {
	src := NewSerializer(nil)
	src.WriteString("append-source-payload-abcdefghijklmnopqrstuvwxyz")
	dst := NewSerializer(make([]byte, 0, len(src.Bytes())))

	b.ReportAllocs()
	b.SetBytes(int64(len(src.Bytes())))
	b.ResetTimer()
	for b.Loop() {
		dst.Reset()
		dst.Append(src)
	}
	benchBytesSink = dst.Bytes()
}

func BenchmarkSerializerPutUuid(b *testing.B) {
	uuid := [2]uint64{0x0102030405060708, 0x1112131415161718}
	ser := NewSerializer(make([]byte, 0, 16))

	b.ReportAllocs()
	b.SetBytes(16)
	b.ResetTimer()

	for b.Loop() {
		ser.Reset()
		ser.PutUuid(uuid)
	}
	benchBytesSink = ser.Bytes()
}

func BenchmarkSerializerPutVBytes(b *testing.B) {
	benchmarkSerializerBytesOp(b, func(ser *Serializer, payload []byte) {
		ser.PutVBytes(payload)
	}, binary.MaxVarintLen64)
}

func BenchmarkSerializerWrite(b *testing.B) {
	benchmarkSerializerBytesOp(b, func(ser *Serializer, payload []byte) {
		_, _ = ser.Write(payload)
	}, 0)
}

func BenchmarkSerializerWriteInts16(b *testing.B) {
	values := make([]int16, 8192)
	for i := range values {
		values[i] = int16(i%32767 - 16384)
	}

	ser := NewSerializer(make([]byte, 0, len(values)*2))
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 2))
	b.ResetTimer()
	for b.Loop() {
		ser.Reset()
		_, _ = ser.WriteInts16(values)
	}
	benchBytesSink = ser.Bytes()
}

func BenchmarkSerializerPutVString(b *testing.B) {
	payload := "put-vstring-benchmark-payload-abcdefghijklmnopqrstuvwxyz"
	ser := NewSerializer(make([]byte, 0, len(payload)+binary.MaxVarintLen64))
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for b.Loop() {
		ser.Reset()
		ser.PutVString(payload)
	}
	benchBytesSink = ser.Bytes()
}

func BenchmarkSerializerReadUIntBits(b *testing.B) {
	sizes := []uintptr{1, 2, 4, 8}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			wr := NewSerializer(nil)
			for i := 0; i < 4096; i++ {
				wr.writeIntBits(int64(i), sz)
			}
			rd := NewSerializer(wr.Bytes())

			b.ReportAllocs()
			b.SetBytes(int64(sz))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if rd.pos+int(sz) > len(rd.buf) {
					rd.pos = 0
				}
				benchUint64Sink ^= rd.readUIntBits(sz)
			}
		})
	}
}
