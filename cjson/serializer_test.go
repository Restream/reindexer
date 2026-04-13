package cjson

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializerPutUuidLittleEndianLayout(t *testing.T) {
	ser := NewSerializer(nil)
	uuid := [2]uint64{0x0102030405060708, 0x1112131415161718}
	ser.PutUuid(uuid)

	got := ser.Bytes()
	require.Len(t, got, 16)
	assert.Equal(t, uuid[0], binary.LittleEndian.Uint64(got[:8]))
	assert.Equal(t, uuid[1], binary.LittleEndian.Uint64(got[8:]))
}

func TestSerializerPutFloatVectorEncoding(t *testing.T) {
	ser := NewSerializer(nil)
	vec := []float32{1.5, -2.25, 7.0}
	ser.PutFloatVector(vec)
	got := ser.Bytes()

	wantHeader := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(wantHeader, uint64(len(vec))<<1)
	require.Len(t, got, n+len(vec)*4)
	require.Equal(t, wantHeader[:n], got[:n])

	for i, f := range vec {
		off := n + i*4
		gotBits := binary.LittleEndian.Uint32(got[off : off+4])
		wantBits := math.Float32bits(f)
		assert.Equal(t, wantBits, gotBits, "float bits mismatch at index %d", i)
	}
}

func TestSerializerWriteIntsAndWriteInts16(t *testing.T) {
	t.Run("WriteInts16", func(t *testing.T) {
		ser := NewSerializer(nil)
		values := []int16{1, -2, 32767, -32768}
		written, err := ser.WriteInts16(values)
		require.NoError(t, err)
		require.Equal(t, len(values)*2, written)

		got := ser.Bytes()
		require.Len(t, got, len(values)*2)
		for i, v := range values {
			off := i * 2
			assert.Equal(t, uint16(v), binary.LittleEndian.Uint16(got[off:off+2]), "value mismatch at index %d", i)
		}
	})

	t.Run("WriteInts", func(t *testing.T) {
		ser := NewSerializer(nil)
		values := []int{1, -2, 3, -4, 1 << 20}
		written, err := ser.WriteInts(values)
		require.NoError(t, err)
		require.Equal(t, len(values)*8, written)

		got := ser.Bytes()
		require.Len(t, got, len(values)*8)
		for i, v := range values {
			off := i * 8
			assert.Equal(t, uint64(v), binary.LittleEndian.Uint64(got[off:off+8]), "value mismatch at index %d", i)
		}
	})
}

func TestSerializerReadWriteFixedWidthRoundTrip(t *testing.T) {
	ser := NewSerializer(nil)
	ser.PutUInt8(7).PutUInt16(513).PutUInt32(1024).PutUInt64(1<<40 + 5)
	ser.PutFloat32(1.25).PutDouble(-15.5)

	rd := NewSerializer(ser.Bytes())
	assert.EqualValues(t, 7, rd.ReadUIntBits(1))
	assert.EqualValues(t, 513, rd.GetUInt16())
	assert.EqualValues(t, 1024, rd.GetUInt32())
	assert.EqualValues(t, 1<<40+5, rd.GetUInt64())
	assert.Equal(t, float32(1.25), rd.GetFloat32())
	assert.Equal(t, -15.5, rd.GetDouble())
}

func TestSerializerWriteReadFloatArrays(t *testing.T) {
	f32 := []float32{1.25, -2.5, 3.75}
	f64 := []float64{10.125, -11.5, 12.875}

	ser := NewSerializer(nil)
	_, err := ser.WriteFloat32s(f32)
	require.NoError(t, err)
	_, err = ser.WriteFloat64s(f64)
	require.NoError(t, err)

	rd := NewSerializer(ser.Bytes())
	got32 := make([]float32, len(f32))
	got64 := make([]float64, len(f64))
	rd.ReadFloat32s(got32)
	rd.ReadFloat64s(got64)

	assert.Equal(t, f32, got32)
	assert.Equal(t, f64, got64)
}
