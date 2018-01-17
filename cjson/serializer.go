package cjson

import (
	"encoding/binary"
	"math"
	"unsafe"

	"sync"

	"github.com/restream/reindexer/bindings"
)

import "C"

type CInt C.int
type CUInt8 bindings.CUInt8
type CInt16 bindings.CInt16
type CInt8 bindings.CInt8

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

func (s *Serializer) Append(s2 Serializer) {
	sl := len(s2.buf)
	l := len(s.buf)
	s.grow(sl)
	for i := 0; i < sl; i++ {

		s.buf[i+l] = s2.buf[i]
	}
}

func (s *Serializer) PutCInt(v int) *Serializer {
	vx := CInt(v)
	s.writeIntBits(int64(vx), unsafe.Sizeof(vx))
	return s
}

func (s *Serializer) PutBool(v bool) *Serializer {
	vx := CInt(0)
	if v {
		vx = CInt(1)
	}
	s.writeIntBits(int64(vx), unsafe.Sizeof(vx))
	return s
}

func (s *Serializer) PutInt64(v int64) *Serializer {
	s.writeIntBits(v, unsafe.Sizeof(v))
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

func (s *Serializer) PutString(vx string) *Serializer {

	v := []byte(vx)
	s.PutBytes(v)
	return s
}

func (s *Serializer) WriteString(vx string) *Serializer {

	v := []byte(vx)
	s.Write(v)
	return s

}

func (s *Serializer) PutBytes(v []byte) *Serializer {
	sl := len(v)
	s.PutCInt(sl)
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

func (s *Serializer) PutVarInt(v int64) {
	l := len(s.buf)
	s.grow(10)
	rl := binary.PutVarint(s.buf[l:], v)
	s.buf = s.buf[:rl+l]
}

func (s *Serializer) PutVarUInt(v uint64) {
	l := len(s.buf)
	s.grow(10)
	rl := binary.PutUvarint(s.buf[l:], v)
	s.buf = s.buf[:rl+l]
}

func (s *Serializer) PutVString(v string) {
	sl := len(v)

	s.PutVarUInt(uint64(sl))
	l := len(s.buf)
	s.grow(sl)

	for i := 0; i < sl; i++ {
		s.buf[i+l] = v[i]
	}
}

func (s *Serializer) GetCInt() (v int) {
	vx := CInt(0)
	return int(s.readIntBits(unsafe.Sizeof(vx)))
}
func (s *Serializer) GetCUInt8() (v int) {
	vx := CUInt8(0)
	return int(s.readIntBits(unsafe.Sizeof(vx)))
}
func (s *Serializer) GetCInt8() (v int) {
	vx := CInt8(0)
	return int(s.readIntBits(unsafe.Sizeof(vx)))
}
func (s *Serializer) GetCInt16() (v int) {
	vx := CInt16(0)
	return int(s.readIntBits(unsafe.Sizeof(vx)))
}

func (s *Serializer) SkipCInts(cnt int) {

	l := cnt * int(unsafe.Sizeof(CInt(0)))
	if s.pos+l > len(s.buf) {
		panic(0)
	}
	s.pos += l
}

func (s *Serializer) GetInt64() (v int64) {
	return s.readIntBits(unsafe.Sizeof(v))
}

func (s *Serializer) GetUInt64() (v uint64) {
	return s.readUIntBits(unsafe.Sizeof(v))
}

func (s *Serializer) GetDouble() (v float64) {
	return math.Float64frombits(uint64(s.readIntBits(unsafe.Sizeof(v))))
}

func (s *Serializer) GetBytes() (v []byte) {
	l := s.GetCInt()
	if s.pos+l > len(s.buf) {
		panic(0)
	}

	v = s.buf[s.pos : s.pos+l]
	//log.Printf("lll=%d,pos=%d,len(v)=%d,len(buf)=%d", l, s.pos, len(v), len(s.buf))
	s.pos += l
	return v
}

func (s *Serializer) readIntBits(sz uintptr) (v int64) {
	if s.pos+int(sz) > len(s.buf) {
		panic(0)
	}

	for i := int(sz) - 1; i >= 0; i-- {
		v = (int64(s.buf[i+s.pos]) & 0xFF) | (v << 8)
	}
	s.pos += int(sz)
	return v
}
func (s *Serializer) readUIntBits(sz uintptr) (v uint64) {
	if s.pos+int(sz) > len(s.buf) {
		panic(0)
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

func (s *Serializer) GetVarInt() int64 {
	ret, l := binary.Varint(s.buf[s.pos:])
	s.pos += l
	return ret
}

func (s *Serializer) GetVString() (v string) {
	l := int(s.GetVarUInt())
	if s.pos+l > len(s.buf) {
		panic(0)
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
