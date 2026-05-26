package cjson

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"sync"
)

var serPool sync.Pool
var isLittleEndian = func() bool {
	var v uint16 = 0x0102
	return *(*byte)(unsafe.Pointer(&v)) == 0x02
}()

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
	copy(s.buf[l:], s2.buf)
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
	l := len(s.buf)
	s.grow(16)
	binary.LittleEndian.PutUint64(s.buf[l:], v[0])
	binary.LittleEndian.PutUint64(s.buf[l+8:], v[1])
	return s
}

func (s *Serializer) PutFloatVector(vec []float32) *Serializer {
	s.PutVarUInt(uint64(len(vec)) << 1)
	l := len(s.buf)
	s.grow(len(vec) * 4)
	if len(vec) > 0 && isLittleEndian {
		vs := unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*4)
		copy(s.buf[l:], vs)
		return s
	}
	for i, value := range vec {
		binary.LittleEndian.PutUint32(s.buf[l+i*4:], math.Float32bits(value))
	}
	return s
}

func (s *Serializer) PutFloat32(v float32) *Serializer {
	s.writeIntBits(int64(math.Float32bits(v)), unsafe.Sizeof(v))
	return s
}

func (s *Serializer) PutDouble(v float64) *Serializer {
	l := len(s.buf)
	s.grow(8)
	binary.LittleEndian.PutUint64(s.buf[l:], math.Float64bits(v))
	return s
}

func (s *Serializer) PutValue(v reflect.Value) error {
	k := v.Kind()
	if k == reflect.Ptr || k == reflect.Interface {
		v = v.Elem()
		k = v.Kind()
	}
	switch k {
	case reflect.Bool:
		s.PutVarCUInt(valueBool)
		if v.Bool() {
			s.PutVarUInt(1)
		} else {
			s.PutVarUInt(0)
		}
	case reflect.Uint:
		if unsafe.Sizeof(int(0)) == unsafe.Sizeof(int64(0)) {
			s.PutVarCUInt(valueInt64)
		} else {
			s.PutVarCUInt(valueInt)
		}

		s.PutVarInt(int64(v.Uint()))
	case reflect.Int:
		if unsafe.Sizeof(int(0)) == unsafe.Sizeof(int64(0)) {
			s.PutVarCUInt(valueInt64)
		} else {
			s.PutVarCUInt(valueInt)
		}
		s.PutVarInt(v.Int())
	case reflect.Int16, reflect.Int32, reflect.Int8:
		s.PutVarCUInt(valueInt)
		s.PutVarInt(v.Int())
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		s.PutVarCUInt(valueInt)
		s.PutVarInt(int64(v.Uint()))
	case reflect.Int64:
		s.PutVarCUInt(valueInt64)
		s.PutVarInt(v.Int())
	case reflect.Uint64:
		s.PutVarCUInt(valueInt64)
		s.PutVarInt(int64(v.Uint()))
	case reflect.String:
		s.PutVarCUInt(valueString)
		s.PutVString(v.String())
	case reflect.Float32, reflect.Float64:
		s.PutVarCUInt(valueDouble)
		s.PutDouble(v.Float())
	case reflect.Slice, reflect.Array:
		s.PutVarCUInt(valueTuple)
		s.PutVarCUInt(v.Len())
		for i := 0; i < v.Len(); i++ {
			s.PutValue(v.Index(i))
		}
	default:
		panic(fmt.Errorf("rq: Invalid reflection type %s", v.Kind().String()))
	}
	return nil
}

func (s *Serializer) writeIntBits(v int64, sz uintptr) {
	l := len(s.buf)
	s.grow(int(sz))
	switch sz {
	case 1:
		s.buf[l] = byte(v)
	case 2:
		binary.LittleEndian.PutUint16(s.buf[l:], uint16(v))
	case 4:
		binary.LittleEndian.PutUint32(s.buf[l:], uint32(v))
	case 8:
		binary.LittleEndian.PutUint64(s.buf[l:], uint64(v))
	default:
		for i := 0; i < int(sz); i++ {
			s.buf[i+l] = byte(v)
			v = v >> 8
		}
	}
}

func (s *Serializer) WriteString(vx string) *Serializer {
	l := len(s.buf)
	s.grow(len(vx))
	copy(s.buf[l:], vx)
	return s
}

func (s *Serializer) PutVBytes(v []byte) *Serializer {
	sl := len(v)
	s.PutVarUInt(uint64(sl))
	l := len(s.buf)
	s.grow(sl)
	copy(s.buf[l:], v)
	return s
}

func (s *Serializer) Write(v []byte) (n int, err error) {
	sl := len(v)
	l := len(s.buf)
	s.grow(sl)
	copy(s.buf[l:], v)
	return sl, nil
}

func (s *Serializer) WriteInts16(v []int16) (n int, err error) {
	sl := len(v)
	slBytes := sl * 2
	l := len(s.buf)
	s.grow(slBytes)
	if sl > 0 && isLittleEndian {
		vs := unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), slBytes)
		copy(s.buf[l:], vs)
		return slBytes, nil
	}

	for i := 0; i < sl; i++ {
		v64 := uint16(v[i])
		binary.LittleEndian.PutUint16(s.buf[l:], v64)
		l += 2
	}
	return slBytes, nil
}

func (s *Serializer) WriteInts(v []int) (n int, err error) {
	sl := len(v)
	slBytes := sl * 8
	l := len(s.buf)
	s.grow(slBytes)
	if sl > 0 && unsafe.Sizeof(int(0)) == 8 && isLittleEndian {
		vs := unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), slBytes)
		copy(s.buf[l:], vs)
		return slBytes, nil
	}

	for i := 0; i < sl; i++ {
		v64 := uint64(v[i])
		binary.LittleEndian.PutUint64(s.buf[l:], v64)
		l += 8
	}
	return slBytes, nil
}
func (s *Serializer) WriteFloat32s(v []float32) (n int, err error) {
	sl := len(v)
	slBytes := sl * 4
	l := len(s.buf)
	s.grow(slBytes)
	if sl > 0 && isLittleEndian {
		vs := unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), slBytes)
		copy(s.buf[l:], vs)
		return slBytes, nil
	}
	for i := 0; i < sl; i++ {
		binary.LittleEndian.PutUint32(s.buf[l:], math.Float32bits(v[i]))
		l += 4
	}
	return slBytes, nil
}

func (s *Serializer) WriteFloat64s(v []float64) (n int, err error) {
	sl := len(v)
	slBytes := sl * 8
	l := len(s.buf)
	s.grow(slBytes)
	if sl > 0 && isLittleEndian {
		vs := unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), slBytes)
		copy(s.buf[l:], vs)
		return slBytes, nil
	}
	for i := 0; i < sl; i++ {
		binary.LittleEndian.PutUint64(s.buf[l:], math.Float64bits(v[i]))
		l += 8
	}
	return slBytes, nil
}

func (s *Serializer) PutVarInt(v int64) {
	uv := uint64(v) << 1
	if v < 0 {
		uv = ^uv
	}
	s.PutVarUInt(uv)
}

func (s *Serializer) PutVarUInt(v uint64) *Serializer {
	if v < 0x80 {
		l := len(s.buf)
		s.grow(1)
		s.buf[l] = byte(v)
		return s
	}
	if v < 0x4000 {
		l := len(s.buf)
		s.grow(2)
		s.buf[l] = byte(v) | 0x80
		s.buf[l+1] = byte(v >> 7)
		return s
	}
	if v < 0x200000 {
		l := len(s.buf)
		s.grow(3)
		s.buf[l] = byte(v) | 0x80
		s.buf[l+1] = byte(v>>7) | 0x80
		s.buf[l+2] = byte(v >> 14)
		return s
	}
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
	copy(s.buf[l:], v)
	return s
}
func (s *Serializer) Truncate(pos int) {
	s.buf = s.buf[:pos]
}

func (s *Serializer) TruncateStart(pos int) {
	s.buf = s.buf[pos:]
}

func (s *Serializer) GetUInt16() (v uint16) {
	pos := s.pos
	if pos+2 > len(s.buf) {
		s.readSizePanic(2)
	}
	v = binary.LittleEndian.Uint16(s.buf[pos:])
	s.pos = pos + 2
	return v
}

func (s *Serializer) GetUInt32() (v uint32) {
	pos := s.pos
	if pos+4 > len(s.buf) {
		s.readSizePanic(4)
	}
	v = binary.LittleEndian.Uint32(s.buf[pos:])
	s.pos = pos + 4
	return v
}

func (s *Serializer) GetCArrayTag() carraytag {
	return carraytag(s.GetUInt32())
}

func (s *Serializer) GetUInt64() (v uint64) {
	pos := s.pos
	if pos+8 > len(s.buf) {
		s.readSizePanic(8)
	}
	v = binary.LittleEndian.Uint64(s.buf[pos:])
	s.pos = pos + 8
	return v
}

func (s *Serializer) GetDouble() (v float64) {
	pos := s.pos
	if pos+8 > len(s.buf) {
		s.readSizePanic(8)
	}
	v = math.Float64frombits(binary.LittleEndian.Uint64(s.buf[pos:]))
	s.pos = pos + 8
	return v
}

func (s *Serializer) GetFloat32() (v float32) {
	pos := s.pos
	if pos+4 > len(s.buf) {
		s.readSizePanic(4)
	}
	v = math.Float32frombits(binary.LittleEndian.Uint32(s.buf[pos:]))
	s.pos = pos + 4
	return v
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

func (s *Serializer) SkipVString() {
	l := int(s.GetVarUInt())
	if s.pos+l > len(s.buf) {
		panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", l, len(s.buf)-s.pos))
	}
	s.pos += l
}

//go:noinline
func (s *Serializer) readSizePanic(sz int) {
	panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", s.pos+sz, len(s.buf)-s.pos))
}

func (s *Serializer) GetVarUInt() uint64 {
	if s.pos < len(s.buf) {
		if b := s.buf[s.pos]; b < 0x80 {
			s.pos++
			return uint64(b)
		}
	}
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
	if s.pos < len(s.buf) {
		if b := s.buf[s.pos]; b < 0x80 {
			s.pos++
			uv := uint64(b)
			return int64(uv>>1) ^ -int64(uv&1)
		}
	}
	pos := s.pos
	if pos+1 < len(s.buf) {
		b0 := s.buf[pos]
		b1 := s.buf[pos+1]
		if b1 < 0x80 {
			s.pos = pos + 2
			uv := uint64(b0&0x7f) | uint64(b1)<<7
			return int64(uv>>1) ^ -int64(uv&1)
		}
		if pos+2 < len(s.buf) {
			b2 := s.buf[pos+2]
			if b2 < 0x80 {
				s.pos = pos + 3
				uv := uint64(b0&0x7f) | uint64(b1&0x7f)<<7 | uint64(b2)<<14
				return int64(uv>>1) ^ -int64(uv&1)
			}
		}
	}
	return s.getVarIntSlow()
}

//go:noinline
func (s *Serializer) getVarIntSlow() int64 {
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
func (s *Serializer) ReadFloat32s(dst []float32) {
	sl := len(dst)
	slBytes := sl * 4
	if s.pos+slBytes > len(s.buf) {
		panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", s.pos+slBytes, len(s.buf)-s.pos))
	}
	if sl == 0 {
		return
	}
	if isLittleEndian {
		vs := unsafe.Slice((*byte)(unsafe.Pointer(&dst[0])), slBytes)
		copy(vs, s.buf[s.pos:s.pos+slBytes])
		s.pos += slBytes
		return
	}
	for i := 0; i < sl; i++ {
		dst[i] = s.GetFloat32()
	}
}

func (s *Serializer) ReadFloat64s(dst []float64) {
	sl := len(dst)
	slBytes := sl * 8
	if s.pos+slBytes > len(s.buf) {
		panic(fmt.Errorf("Internal error: serializer need %d bytes, but only %d available", s.pos+slBytes, len(s.buf)-s.pos))
	}
	if sl == 0 {
		return
	}
	if isLittleEndian {
		vs := unsafe.Slice((*byte)(unsafe.Pointer(&dst[0])), slBytes)
		copy(vs, s.buf[s.pos:s.pos+slBytes])
		s.pos += slBytes
		return
	}
	for i := 0; i < sl; i++ {
		dst[i] = s.GetDouble()
	}
}

func (s *Serializer) Eof() bool {
	return s.pos == len(s.buf)
}

func (s *Serializer) Pos() int {
	return s.pos
}
