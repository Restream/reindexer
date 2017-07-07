package reindexer

import (
	"math"
	"reflect"
	"unsafe"

	"fmt"

	"sync"

	"github.com/restream/reindexer/bindings"
)

const (
	valueInt    = bindings.ValueInt
	valueInt64  = bindings.ValueInt64
	valueDouble = bindings.ValueDouble
	valueString = bindings.ValueString
)

var serPool sync.Pool

type serializer struct {
	buf  []byte
	pos  int
	pool bool
}

func newSerializer(buf []byte) serializer {
	return serializer{buf: buf}
}

func newPoolSerializer() *serializer {
	obj := serPool.Get()
	if obj != nil {
		ser := obj.(*serializer)
		ser.pos = 0
		ser.buf = ser.buf[:0]
		return ser
	}
	return &serializer{pool: true}
}

func (s *serializer) close() {
	if s.pool {
		serPool.Put(s)
	}
}

func (s *serializer) GetBuf() []byte {
	return s.buf
}

func (s *serializer) grow(sz int) {
	if s.buf == nil {
		s.buf = make([]byte, 0, 0x80)
	} else if len(s.buf)+sz > cap(s.buf) {
		b := make([]byte, len(s.buf), cap(s.buf)*2+sz+1024)
		copy(b, s.buf)
		s.buf = b
	}
	s.buf = s.buf[0 : len(s.buf)+sz]
}

func (s *serializer) append(s2 serializer) {
	sl := len(s2.buf)
	l := len(s.buf)
	s.grow(sl)
	for i := 0; i < sl; i++ {

		s.buf[i+l] = s2.buf[i]
	}
}

func (s *serializer) writeCInt(v int) *serializer {
	vx := CInt(v)
	s.writeIntBits(int64(vx), unsafe.Sizeof(vx))
	return s
}

func (s *serializer) writeBool(v bool) *serializer {
	vx := CInt(0)
	if v {
		vx = CInt(1)
	}
	s.writeIntBits(int64(vx), unsafe.Sizeof(vx))
	return s
}

func (s *serializer) writeInt64(v int64) *serializer {
	s.writeIntBits(v, unsafe.Sizeof(v))
	return s
}

func (s *serializer) writeDouble(v float64) *serializer {
	s.writeIntBits(int64(math.Float64bits(v)), unsafe.Sizeof(v))
	return s
}

func (s *serializer) writeIntBits(v int64, sz uintptr) {
	l := len(s.buf)
	s.grow(int(sz))
	for i := 0; i < int(sz); i++ {
		s.buf[i+l] = byte(v)
		v = v >> 8
	}
}

func (s *serializer) writeString(vx string) *serializer {

	v := []byte(vx)
	s.writeBytes(v)
	return s

}

func (s *serializer) writeBytes(v []byte) *serializer {
	sl := len(v)
	s.writeCInt(sl)
	l := len(s.buf)
	s.grow(sl)
	for i := 0; i < sl; i++ {
		s.buf[i+l] = v[i]
	}
	return s
}

func (s *serializer) Write(v []byte) (n int, err error) {
	sl := len(v)
	l := len(s.buf)
	s.grow(sl)
	for i := 0; i < sl; i++ {
		s.buf[i+l] = v[i]
	}
	return sl, nil
}

func (s *serializer) writeValue(v reflect.Value) error {

	switch v.Kind() {
	case reflect.Bool:
		s.writeCInt(valueInt)
		if v.Bool() {
			s.writeCInt(1)
		} else {
			s.writeCInt(0)
		}
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int8,
		reflect.Uint, reflect.Uint16, reflect.Uint32:
		s.writeCInt(valueInt)
		s.writeCInt(int(v.Int()))
	case reflect.Int64, reflect.Uint64:
		s.writeCInt(valueInt64)
		s.writeInt64(v.Int())
	case reflect.String:
		s.writeCInt(valueString)
		s.writeString(v.String())
	case reflect.Float32, reflect.Float64:
		s.writeCInt(valueDouble)
		s.writeDouble(v.Float())
	default:
		panic(fmt.Errorf("rq: Invalid reflection type %s", v.Kind().String()))
	}
	return nil
}

func (s *serializer) readCInt() (v int) {
	vx := CInt(0)
	return int(s.readIntBits(unsafe.Sizeof(vx)))
}

func (s *serializer) skipCInts(cnt int) {

	l := cnt * int(unsafe.Sizeof(CInt(0)))
	if s.pos+l > len(s.buf) {
		panic(0)
	}
	s.pos += l
}

func (s *serializer) readInt64() (v int64) {
	return s.readIntBits(unsafe.Sizeof(v))
}

func (s *serializer) readDouble() (v float64) {
	return math.Float64frombits(uint64(s.readIntBits(unsafe.Sizeof(v))))
}

func (s *serializer) readBytes() (v []byte) {
	l := s.readCInt()
	if s.pos+l > len(s.buf) {
		panic(0)
	}

	v = s.buf[s.pos : s.pos+l]
	//log.Printf("lll=%d,pos=%d,len(v)=%d,len(buf)=%d", l, s.pos, len(v), len(s.buf))
	s.pos += l
	return v
}

func (s *serializer) readIntBits(sz uintptr) (v int64) {
	if s.pos+int(sz) > len(s.buf) {
		panic(0)
	}

	for i := int(sz) - 1; i >= 0; i-- {
		v = (int64(s.buf[i+s.pos]) & 0xFF) | (v << 8)
	}
	s.pos += int(sz)
	return v
}

func (s *serializer) eof() bool {
	return s.pos == len(s.buf)
}
