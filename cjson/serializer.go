package cjson

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"sync"
)

var serPool sync.Pool

type Serializer struct {
	buf  []byte
	pos  int
	pool bool
}

func NewPoolSerializer() *Serializer {
	obj := serPool.Get()
	if obj != nil {
		ser := obj.(*Serializer)
		ser.pos = 0
		ser.buf = ser.buf[:0]
		return ser
	}
	return &Serializer{pool: true}
}

func NewSerializer(buf []byte) Serializer {
	return Serializer{buf: buf}
}

func (s *Serializer) Close() {
	if s.pool {
		serPool.Put(s)
	}
}

func (s *Serializer) Bytes() []byte {
	return s.buf
}

func (s *Serializer) reserve(sz int) {
	if len(s.buf)+sz > cap(s.buf) {
		b := make([]byte, len(s.buf), cap(s.buf)*2+sz+1024)
		copy(b, s.buf)
		s.buf = b
	}
}

func (s *Serializer) grow(sz int) {
	if s.buf == nil {
		s.buf = make([]byte, 0, sz+0x80)
	} else if len(s.buf)+sz > cap(s.buf) {
		s.reserve(sz)
	}
	s.buf = s.buf[0 : len(s.buf)+sz]
}

func (s *Serializer) Reset() {
	s.buf = s.buf[:0]
}

func (s *Serializer) Append(s2 Serializer) {
	sl := len(s2.buf)
	l := len(s.buf)
	s.grow(sl)
	for i := 0; i < sl; i++ {

		s.buf[i+l] = s2.buf[i]
	}
}

func (s *Serializer) PutUInt8(v uint8) *Serializer {
	s.writeIntBits(int64(v), unsafe.Sizeof(v))
	return s
}

func (s *Serializer) PutUInt16(v uint16) *Serializer {
	s.writeIntBits(int64(v), unsafe.Sizeof(v))
	return s
}

func (s *Serializer) PutUInt32(v uint32) *Serializer {
	s.writeIntBits(int64(v), unsafe.Sizeof(v))
	return s
}

func (s *Serializer) PutUInt64(v uint64) *Serializer {
	s.writeIntBits(int64(v), unsafe.Sizeof(v))
	return s
}

func (s *Serializer) PutUuid(v [2]uint64) *Serializer {
	s.PutUInt64(v[0])
	s.PutUInt64(v[1])
	return s
}

func (s *Serializer) PutFloatVector(vec []float32) *Serializer {
	s.PutVarUInt(uint64(len(vec)) << 1)
	for _, value := range vec {
		s.writeIntBits(int64(math.Float32bits(value)), unsafe.Sizeof(value))
	}
	return s
}

func (s *Serializer) PutFloat32(v float32) *Serializer {
	s.writeIntBits(int64(math.Float32bits(v)), unsafe.Sizeof(v))
	return s
}

func (s *Serializer) PutDouble(v float64) *Serializer {
	s.writeIntBits(int64(math.Float64bits(v)), unsafe.Sizeof(v))
	return s
}

func (s *Serializer) writeIntBits(v int64, sz uintptr) {
	l := len(s.buf)
	s.grow(int(sz))
	for i := 0; i < int(sz); i++ {
		s.buf[i+l] = byte(v)
		v = v >> 8
	}
}

func (s *Serializer) WriteString(vx string) *Serializer {
	v := []byte(vx)
	s.Write(v)
	return s
}

func (s *Serializer) PutVBytes(v []byte) *Serializer {
	sl := len(v)
	s.PutVarUInt(uint64(sl))
	l := len(s.buf)
	s.grow(sl)
	for i := 0; i < sl; i++ {
		s.buf[i+l] = v[i]
	}
	return s
}

func (s *Serializer) Write(v []byte) (n int, err error) {
	sl := len(v)
	l := len(s.buf)
	s.grow(sl)
	for i := 0; i < sl; i++ {
		s.buf[i+l] = v[i]
	}
	return sl, nil
}

func (s *Serializer) WriteInts16(v []int16) (n int, err error) {
	sl := len(v)
	slBytes := sl * 2
	l := len(s.buf)
	s.grow(slBytes)
	for i := 0; i < sl; i++ {
		v64 := uint16(v[i])
		s.buf[l] = byte(v64)
		l += 1
		s.buf[l] = byte(v64 >> 8)
		l += 1
	}
	return slBytes, nil
}

func (s *Serializer) WriteInts(v []int) (n int, err error) {
	sl := len(v)
	slBytes := sl * 8
	l := len(s.buf)
	s.grow(slBytes)
	for i := 0; i < sl; i++ {
		v64 := uint64(v[i])
		s.buf[l] = byte(v64)
		l += 1
		s.buf[l] = byte(v64 >> 8)
		l += 1
		s.buf[l] = byte(v64 >> 16)
		l += 1
		s.buf[l] = byte(v64 >> 24)
		l += 1
		s.buf[l] = byte(v64 >> 32)
		l += 1
		s.buf[l] = byte(v64 >> 40)
		l += 1
		s.buf[l] = byte(v64 >> 48)
		l += 1
		s.buf[l] = byte(v64 >> 56)
		l += 1
	}
	return slBytes, nil
}

func (s *Serializer) PutVarInt(v int64) {
	l := len(s.buf)
	s.grow(10)
	rl := binary.PutVarint(s.buf[l:], v)
	s.buf = s.buf[:rl+l]
}

func (s *Serializer) PutVarUInt(v uint64) *Serializer {
	l := len(s.buf)
	s.grow(10)
	rl := binary.PutUvarint(s.buf[l:], v)
	s.buf = s.buf[:rl+l]
	return s
}

func (s *Serializer) PutCTag(v ctag) *Serializer {
	s.PutVarUInt(uint64(v))
	return s
}

func (s *Serializer) PutCArrayTag(v carraytag) *Serializer {
	s.PutUInt32(uint32(v))
	return s
}

func (s *Serializer) PutVarCUInt(v int) *Serializer {
	return s.PutVarUInt(uint64(v))
}

func (s *Serializer) PutVString(v string) *Serializer {
	sl := len(v)

	s.PutVarUInt(uint64(sl))
	l := len(s.buf)
	s.grow(sl)

	for i := 0; i < sl; i++ {
		s.buf[i+l] = v[i]
	}
	return s
}
func (s *Serializer) Truncate(pos int) {
	s.buf = s.buf[:pos]
}

func (s *Serializer) TruncateStart(pos int) {
	s.buf = s.buf[pos:]
}

func (s *Serializer) GetUInt16() (v uint16) {
	return uint16(s.readIntBits(unsafe.Sizeof(v)))
}

func (s *Serializer) GetUInt32() (v uint32) {
	return uint32(s.readIntBits(unsafe.Sizeof(v)))
}

func (s *Serializer) GetCArrayTag() carraytag {
	return carraytag(s.GetUInt32())
}

func (s *Serializer) GetUInt64() (v uint64) {
	return s.readUIntBits(unsafe.Sizeof(v))
}

func (s *Serializer) GetDouble() (v float64) {
	return math.Float64frombits(uint64(s.readIntBits(unsafe.Sizeof(v))))
}

func (s *Serializer) GetFloat32() (v float32) {
	return math.Float32frombits(uint32(s.readIntBits(unsafe.Sizeof(v))))
}

func (s *Serializer) GetBytes() (v []byte) {
	l := int(s.GetUInt32())
	if s.pos+l > len(s.buf) {
		panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", l, len(s.buf)-s.pos))
	}

	v = s.buf[s.pos : s.pos+l]
	s.pos += l
	return v
}
func (s *Serializer) GetVBytes() (v []byte) {
	l := int(s.GetVarUInt())
	if s.pos+l > len(s.buf) {
		panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", l, len(s.buf)-s.pos))
	}

	v = s.buf[s.pos : s.pos+l]
	s.pos += l
	return v
}

func (s *Serializer) readIntBits(sz uintptr) (v int64) {
	if s.pos+int(sz) > len(s.buf) {
		panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", s.pos+int(sz), len(s.buf)-s.pos))
	}

	for i := int(sz) - 1; i >= 0; i-- {
		v = (int64(s.buf[i+s.pos]) & 0xFF) | (v << 8)
	}
	s.pos += int(sz)
	return v
}
func (s *Serializer) readUIntBits(sz uintptr) (v uint64) {
	if s.pos+int(sz) > len(s.buf) {
		panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", s.pos+int(sz), len(s.buf)-s.pos))
	}

	for i := int(sz) - 1; i >= 0; i-- {
		v = (uint64(s.buf[i+s.pos]) & 0xFF) | (v << 8)
	}
	s.pos += int(sz)
	return v
}

func (s *Serializer) GetVarUInt() uint64 {
	ret, l := binary.Uvarint(s.buf[s.pos:])
	s.pos += l
	return ret
}

func (s *Serializer) GetCTag() ctag {
	return ctag(s.GetVarUInt())
}

func (s *Serializer) GetUuid() string {
	v0 := s.GetUInt64()
	v1 := s.GetUInt64()
	return createUuid([2]uint64{v0, v1})
}

func (s *Serializer) GetVarInt() int64 {
	ret, l := binary.Varint(s.buf[s.pos:])
	s.pos += l
	return ret
}

func (s *Serializer) GetVString() (v string) {
	l := int(s.GetVarUInt())
	if s.pos+l > len(s.buf) {
		panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", l, len(s.buf)-s.pos))
	}

	v = string(s.buf[s.pos : s.pos+l])
	s.pos += l
	return v
}

func (s *Serializer) Eof() bool {
	return s.pos == len(s.buf)
}

func (s *Serializer) Pos() int {
	return s.pos
}
