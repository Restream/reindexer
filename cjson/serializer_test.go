package cjson

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestSerializerPutUuidLittleEndianLayout(t *testing.T) {
	ser := NewSerializer(nil)
	uuid := [2]uint64{0x0102030405060708, 0x1112131415161718}
	ser.PutUuid(uuid)

	got := ser.Bytes()
	if len(got) != 16 {
		t.Fatalf("unexpected length: got=%d want=16", len(got))
	}

	if binary.LittleEndian.Uint64(got[:8]) != uuid[0] {
		t.Fatalf("first uint64 mismatch: got=%x want=%x", binary.LittleEndian.Uint64(got[:8]), uuid[0])
	}
	if binary.LittleEndian.Uint64(got[8:]) != uuid[1] {
		t.Fatalf("second uint64 mismatch: got=%x want=%x", binary.LittleEndian.Uint64(got[8:]), uuid[1])
	}
}

func TestSerializerPutFloatVectorEncoding(t *testing.T) {
	ser := NewSerializer(nil)
	vec := []float32{1.5, -2.25, 7.0}
	ser.PutFloatVector(vec)
	got := ser.Bytes()

	wantHeader := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(wantHeader, uint64(len(vec))<<1)
	if len(got) != n+len(vec)*4 {
		t.Fatalf("unexpected length: got=%d want=%d", len(got), n+len(vec)*4)
	}
	if string(got[:n]) != string(wantHeader[:n]) {
		t.Fatalf("header mismatch: got=%v want=%v", got[:n], wantHeader[:n])
	}

	for i, f := range vec {
		off := n + i*4
		gotBits := binary.LittleEndian.Uint32(got[off : off+4])
		wantBits := math.Float32bits(f)
		if gotBits != wantBits {
			t.Fatalf("float bits mismatch at %d: got=%x want=%x", i, gotBits, wantBits)
		}
	}
}

func TestSerializerWriteIntsAndWriteInts16(t *testing.T) {
	t.Run("WriteInts16", func(t *testing.T) {
		ser := NewSerializer(nil)
		values := []int16{1, -2, 32767, -32768}
		written, err := ser.WriteInts16(values)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if written != len(values)*2 {
			t.Fatalf("written mismatch: got=%d want=%d", written, len(values)*2)
		}

		got := ser.Bytes()
		if len(got) != len(values)*2 {
			t.Fatalf("len mismatch: got=%d want=%d", len(got), len(values)*2)
		}
		for i, v := range values {
			off := i * 2
			if binary.LittleEndian.Uint16(got[off:off+2]) != uint16(v) {
				t.Fatalf("value mismatch at %d: got=%d want=%d", i, binary.LittleEndian.Uint16(got[off:off+2]), uint16(v))
			}
		}
	})

	t.Run("WriteInts", func(t *testing.T) {
		ser := NewSerializer(nil)
		values := []int{1, -2, 3, -4, 1 << 20}
		written, err := ser.WriteInts(values)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if written != len(values)*8 {
			t.Fatalf("written mismatch: got=%d want=%d", written, len(values)*8)
		}

		got := ser.Bytes()
		if len(got) != len(values)*8 {
			t.Fatalf("len mismatch: got=%d want=%d", len(got), len(values)*8)
		}
		for i, v := range values {
			off := i * 8
			if binary.LittleEndian.Uint64(got[off:off+8]) != uint64(v) {
				t.Fatalf("value mismatch at %d: got=%d want=%d", i, binary.LittleEndian.Uint64(got[off:off+8]), uint64(v))
			}
		}
	})
}

func TestSerializerReadWriteFixedWidthRoundTrip(t *testing.T) {
	ser := NewSerializer(nil)
	ser.PutUInt8(7).PutUInt16(513).PutUInt32(1024).PutUInt64(1<<40 + 5)
	ser.PutFloat32(1.25).PutDouble(-15.5)

	rd := NewSerializer(ser.Bytes())
	if got := rd.readUIntBits(1); got != 7 {
		t.Fatalf("unexpected uint8: got=%d want=7", got)
	}
	if got := rd.GetUInt16(); got != 513 {
		t.Fatalf("unexpected uint16: got=%d want=513", got)
	}
	if got := rd.GetUInt32(); got != 1024 {
		t.Fatalf("unexpected uint32: got=%d want=1024", got)
	}
	if got := rd.GetUInt64(); got != 1<<40+5 {
		t.Fatalf("unexpected uint64: got=%d want=%d", got, uint64(1<<40+5))
	}
	if got := rd.GetFloat32(); got != 1.25 {
		t.Fatalf("unexpected float32: got=%f want=%f", got, 1.25)
	}
	if got := rd.GetDouble(); got != -15.5 {
		t.Fatalf("unexpected float64: got=%f want=%f", got, -15.5)
	}
}

func TestSerializerWriteReadFloatArrays(t *testing.T) {
	f32 := []float32{1.25, -2.5, 3.75}
	f64 := []float64{10.125, -11.5, 12.875}

	ser := NewSerializer(nil)
	if _, err := ser.WriteFloat32s(f32); err != nil {
		t.Fatalf("WriteFloat32s failed: %v", err)
	}
	if _, err := ser.WriteFloat64s(f64); err != nil {
		t.Fatalf("WriteFloat64s failed: %v", err)
	}

	rd := NewSerializer(ser.Bytes())
	got32 := make([]float32, len(f32))
	got64 := make([]float64, len(f64))
	rd.ReadFloat32s(got32)
	rd.ReadFloat64s(got64)

	for i := range f32 {
		if got32[i] != f32[i] {
			t.Fatalf("float32 mismatch at %d: got=%f want=%f", i, got32[i], f32[i])
		}
	}
	for i := range f64 {
		if got64[i] != f64[i] {
			t.Fatalf("float64 mismatch at %d: got=%f want=%f", i, got64[i], f64[i])
		}
	}
}

func BenchmarkSerializerEncodeTypicalDocument(b *testing.B) {
	tags := []string{"books", "science", "math", "history"}
	scores := []int{10, 42, 17, 5, 99, 100, 77, 54}
	embedding := make([]float32, 256)
	for i := range embedding {
		embedding[i] = float32(i) / 10
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ser := NewPoolSerializer()
		ser.WriteString("doc-2026-04-12")
		ser.PutVString("The Art of Query Planning")
		ser.PutVarInt(2026)
		ser.PutVarInt(1234567)
		for _, t := range tags {
			ser.PutVString(t)
		}
		_, _ = ser.WriteInts(scores)
		ser.PutFloatVector(embedding)
		ser.Close()
	}
}

func BenchmarkSerializerWriteIntsBatch(b *testing.B) {
	values := make([]int, 4096)
	for i := range values {
		if i%2 == 0 {
			values[i] = i
		} else {
			values[i] = -i
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ser := NewPoolSerializer()
		_, _ = ser.WriteInts(values)
		ser.Close()
	}
}

func BenchmarkSerializerWriteInts16Batch(b *testing.B) {
	values := make([]int16, 8192)
	for i := range values {
		values[i] = int16(i%32767 - 16384)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ser := NewPoolSerializer()
		_, _ = ser.WriteInts16(values)
		ser.Close()
	}
}

func BenchmarkSerializerPutFloatVectorBatch(b *testing.B) {
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i) / 100
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(vec) * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ser := NewPoolSerializer()
		ser.PutFloatVector(vec)
		ser.Close()
	}
}
