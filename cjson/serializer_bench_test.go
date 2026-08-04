package cjson

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

var benchSerializerLen int

func BenchmarkSerializerEncodeTypicalDocument(b *testing.B) {
	tags := []string{"books", "science", "math", "history"}
	scores := []int{10, 42, 17, 5, 99, 100, 77, 54}
	embedding := make([]float32, 256)
	for i := range embedding {
		embedding[i] = float32(i) / 10
	}

	b.ReportAllocs()

	for b.Loop() {
		ser := NewPoolSerializer()
		ser.WriteString("doc-2026-04-12")
		ser.PutVString("The Art of Query Planning")
		ser.PutVarInt(2026)
		ser.PutVarInt(1234567)
		for _, t := range tags {
			ser.PutVString(t)
		}
		_, err := ser.WriteInts(scores)
		require.NoError(b, err)
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

	for b.Loop() {
		ser := NewPoolSerializer()
		_, err := ser.WriteInts(values)
		require.NoError(b, err)
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

	for b.Loop() {
		ser := NewPoolSerializer()
		_, err := ser.WriteInts16(values)
		require.NoError(b, err)
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

	for b.Loop() {
		ser := NewPoolSerializer()
		ser.PutFloatVector(vec)
		ser.Close()
	}
}

func BenchmarkSerializerPutValueInt(b *testing.B) {
	v := reflect.ValueOf(123)
	var buf [32]byte

	b.ReportAllocs()
	for b.Loop() {
		ser := NewSerializer(buf[:0])
		if err := ser.PutValue(v); err != nil {
			b.Fatal(err)
		}
		benchSerializerLen = len(ser.Bytes())
	}
}

func BenchmarkSerializerPutValueString(b *testing.B) {
	v := reflect.ValueOf("benchmark")
	var buf [64]byte

	b.ReportAllocs()
	for b.Loop() {
		ser := NewSerializer(buf[:0])
		if err := ser.PutValue(v); err != nil {
			b.Fatal(err)
		}
		benchSerializerLen = len(ser.Bytes())
	}
}

func BenchmarkSerializerPutValueSlice(b *testing.B) {
	v := reflect.ValueOf([]int{1, 2, 3, 4})
	var buf [128]byte

	b.ReportAllocs()
	for b.Loop() {
		ser := NewSerializer(buf[:0])
		if err := ser.PutValue(v); err != nil {
			b.Fatal(err)
		}
		benchSerializerLen = len(ser.Bytes())
	}
}
