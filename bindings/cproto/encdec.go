package cproto

import (
	"fmt"
	"unsafe"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
	"github.com/golang/snappy"
)

type rpcEncoder struct {
	lastArgsChunckStart int
	ser                 *cjson.Serializer
	enableSnappy        bool
	dedicatedThread     bool
}

type rpcDecoder struct {
	ser cjson.Serializer
}

func newRPCEncoder(cmd int, seq uint32, enableSnappy bool, dedicatedThread bool) rpcEncoder {
	enc := rpcEncoder{ser: cjson.NewPoolSerializer(), enableSnappy: enableSnappy, dedicatedThread: dedicatedThread}
	enc.start(cmd, seq)
	return enc
}

func (r *rpcEncoder) start(cmd int, seq uint32) {
	r.ser.PutUInt32(cprotoMagic)
	var vers uint16 = cprotoVersion
	if r.enableSnappy {
		vers |= cprotoVersionCompressionFlag
	}
	if r.dedicatedThread {
		vers |= cprotoDedicatedThreadFlag
	}
	r.ser.PutUInt16(vers)
	r.ser.PutUInt16(uint16(cmd))
	r.ser.PutUInt32(0) // len
	r.ser.PutUInt32(seq)
	r.startArgsChunck()
}

func (r *rpcEncoder) startArgsChunck() {
	r.lastArgsChunckStart = len(r.ser.Bytes())
	// num args
	r.ser.PutVarUInt(0)
	(*(*uint32)(unsafe.Pointer(&r.ser.Bytes()[8])))++
}

func (r *rpcEncoder) bytesArg(v []byte) {
	r.ser.PutVarUInt(uint64(bindings.ValueString))
	r.ser.PutVBytes(v)
	r.update()
}

func (r *rpcEncoder) stringArg(v string) {
	r.ser.PutVarUInt(uint64(bindings.ValueString))
	r.ser.PutVString(v)
	r.update()
}

func (r *rpcEncoder) intArg(v int) {
	r.ser.PutVarUInt(uint64(bindings.ValueInt))
	r.ser.PutVarInt(int64(v))
	r.update()
}

func (r *rpcEncoder) boolArg(v bool) {
	r.ser.PutVarUInt(uint64(bindings.ValueBool))
	if v {
		r.ser.PutVarUInt(1)
	} else {
		r.ser.PutVarUInt(0)
	}
	r.update()
}

func (r *rpcEncoder) int32ArrArg(v []int32) {
	r.ser.PutVarUInt(uint64(bindings.ValueString))

	aser := cjson.Serializer{}
	aser.PutVarCUInt(len(v))
	for _, e := range v {
		aser.PutVarCUInt(int(e))
	}
	r.ser.PutVBytes(aser.Bytes())
	r.update()
}

func (r *rpcEncoder) int64Arg(v int64) {
	r.ser.PutVarUInt(uint64(bindings.ValueInt64))
	r.ser.PutVarInt(v)
	r.update()
}

func (r *rpcEncoder) update() {
	r.ser.Bytes()[r.lastArgsChunckStart]++
	*(*uint32)(unsafe.Pointer(&r.ser.Bytes()[8])) = uint32(len(r.ser.Bytes()) - cprotoHdrLen)
}

func (r *rpcEncoder) bytes() []byte {
	if r.enableSnappy {
		out := snappy.Encode(nil, r.ser.Bytes()[cprotoHdrLen:])
		r.ser.Truncate(cprotoHdrLen)
		r.ser.Write(out)
		*(*uint16)(unsafe.Pointer(&r.ser.Bytes()[4])) |= cprotoVersionCompressionFlag
		*(*uint32)(unsafe.Pointer(&r.ser.Bytes()[8])) = uint32(len(r.ser.Bytes()) - cprotoHdrLen)
	}
	return r.ser.Bytes()
}

func newRPCDecoder(buf []byte) rpcDecoder {
	return rpcDecoder{ser: cjson.NewSerializer(buf)}
}

func (r *rpcDecoder) errCode() error {
	code := r.ser.GetVarUInt()
	str := r.ser.GetVString()
	if code != 0 {
		return bindings.NewError(str, int(code))
	}
	return nil
}

func (r *rpcDecoder) argsCount() int {
	return int(r.ser.GetVarUInt())
}

func (r *rpcDecoder) bytesArg() []byte {
	t := r.ser.GetVarUInt()
	if t != uint64(bindings.ValueString) {
		panic(fmt.Errorf("cproto: Expected type string, but got %d", t))
	}
	return r.ser.GetVBytes()
}

func (r *rpcDecoder) stringArg() string {
	t := r.ser.GetVarUInt()
	if t != uint64(bindings.ValueString) {
		panic(fmt.Errorf("cproto: Expected type string, but got %d", t))
	}
	return r.ser.GetVString()
}

func (r *rpcDecoder) intArg() int {
	t := r.ser.GetVarUInt()
	if t != uint64(bindings.ValueInt) {
		panic(fmt.Errorf("cproto: Expected type int, but got %d", t))
	}
	return int(r.ser.GetVarInt())
}

func (r *rpcDecoder) intfArg() interface{} {
	t := r.ser.GetVarUInt()
	switch int(t) {
	case bindings.ValueInt:
		return int(r.ser.GetVarInt())
	case bindings.ValueBool:
		return r.ser.GetVarInt() != 0
	case bindings.ValueString:
		return r.ser.GetVBytes()
	case bindings.ValueInt64:
		return r.ser.GetVarInt()
	case bindings.ValueDouble:
		return r.ser.GetDouble()
	}
	panic(fmt.Errorf("cproto: Unexpected arg type %d", t))
}
