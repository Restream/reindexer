package cjson

// typedef struct ArrayHeader {
// 	unsigned offset;
// 	int len;
// } ArrayHeader;
// typedef struct PStringHeader {
// 	void *cstr;
// 	int len;
// } PStringHeader;
import "C"

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/restream/reindexer/bindings"
)

const (
	valueInt    = bindings.ValueInt
	valueInt64  = bindings.ValueInt64
	valueDouble = bindings.ValueDouble
	valueString = bindings.ValueString
)

type payloadFieldType struct {
	Type    int
	Name    string
	Offset  uintptr
	Size    uintptr
	IsArray bool
}

type payloadType struct {
	Fields           []payloadFieldType
	PStringHdrOffset uintptr
}

func (pt *payloadType) Read(ser *Serializer) {
	pt.PStringHdrOffset = uintptr(ser.GetVarUInt())
	fieldsCount := int(ser.GetVarUInt())
	pt.Fields = make([]payloadFieldType, fieldsCount, fieldsCount)

	for i := 0; i < fieldsCount; i++ {
		pt.Fields[i].Type = int(ser.GetVarUInt())
		pt.Fields[i].Name = ser.GetVString()
		pt.Fields[i].Offset = uintptr(ser.GetVarUInt())
		pt.Fields[i].Size = uintptr(ser.GetVarUInt())
		pt.Fields[i].IsArray = ser.GetVarUInt() != 0
	}
}

type payloadIface struct {
	p uintptr
	t *payloadType
}

// direct c reindexer payload manipulation
// very danger!
func (pl *payloadIface) ptr(field, idx, typ int) unsafe.Pointer {

	if pl.p == 0 {
		panic(fmt.Errorf("Null pointer derefernce"))
	}

	f := &pl.t.Fields[field]

	if f.Type != typ {
		panic(fmt.Errorf("Invalid type cast of field '%s' from type '%d' to type '%d'", f.Name, f.Type, typ))
	}

	p := unsafe.Pointer(pl.p + uintptr(f.Offset))

	if !f.IsArray {
		if idx != 0 {
			panic(fmt.Errorf("Trying to acces by index '%d' to non array field '%s'", idx, f.Name))
		}
		return p
	}
	// we have pointer to PayloadValue::Array struct
	arr := (*C.ArrayHeader)(p)
	if idx >= int(arr.len) {
		panic(fmt.Errorf("Index %d is out of bound %d on array field '%s'", idx, int(arr.len), f.Name))
	}
	return unsafe.Pointer(pl.p + uintptr(arr.offset) + uintptr(idx)*f.Size)
}

func (pl *payloadIface) getInt(field, idx int) int {
	p := pl.ptr(field, idx, valueInt)
	return int(*(*C.int)(p))
}

func (pl *payloadIface) getInt64(field, idx int) int64 {
	p := pl.ptr(field, idx, valueInt64)
	return *(*int64)(p)
}

func (pl *payloadIface) getFloat64(field, idx int) float64 {
	p := pl.ptr(field, idx, valueDouble)
	return float64(*(*C.double)(p))
}

func (pl *payloadIface) getBool(field, idx int) bool {
	p := pl.ptr(field, idx, valueInt)
	return *(*C.int)(p) != C.int(0)
}

func (pl *payloadIface) getBytes(field, idx int) []byte {
	p := pl.ptr(field, idx, valueString)
	// p is pointer to p_string. see core/keyvalue/p_string.h

	ppstring := (*(*uintptr)(p)) & ^uintptr((3 << 60))
	strHdr := (*C.PStringHeader)(unsafe.Pointer(ppstring + pl.t.PStringHdrOffset))

	return (*[1 << 30]byte)(strHdr.cstr)[:strHdr.len:strHdr.len]
}

func (pl *payloadIface) getString(field, idx int) string {
	return string(pl.getBytes(field, idx))
}

func (pl *payloadIface) getArrayLen(field int) int {

	if !pl.t.Fields[field].IsArray {
		return 1
	}

	p := unsafe.Pointer(pl.p + uintptr(pl.t.Fields[field].Offset))

	// we have pointer to PayloadValue::Array struct
	return int((*C.ArrayHeader)(p).len)
}

// get c reflect value and set to go reflect valie
func (pl *payloadIface) getValue(field int, idx int, v reflect.Value) {

	k := v.Type().Kind()
	switch pl.t.Fields[field].Type {
	case valueInt:
		switch k {
		case reflect.Bool:
			v.SetBool(pl.getBool(field, idx))
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
			v.SetInt(int64(pl.getInt(field, idx)))
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
			v.SetUint(uint64(pl.getInt(field, idx)))
		default:
			panic(fmt.Errorf("Can't set int to %s", k.String()))
		}
	case valueInt64:
		switch k {
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
			v.SetInt(int64(pl.getInt64(field, idx)))
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
			v.SetUint(uint64(pl.getInt64(field, idx)))
		default:
			panic(fmt.Errorf("Can't set int to %s", k.String()))
		}
	case valueDouble:
		v.SetFloat(pl.getFloat64(field, idx))
	case valueString:
		v.SetString(pl.getString(field, idx))
	default:
		panic(fmt.Errorf("Unknown key value type %d", pl.t.Fields[field].Type))
	}
}

func (pl *payloadIface) getArray(field int, startIdx int, cnt int, v reflect.Value) {

	if cnt == 0 {
		return
	}

	ptr := pl.ptr(field, startIdx, pl.t.Fields[field].Type)
	l := pl.getArrayLen(field) - startIdx

	switch pl.t.Fields[field].Type {
	case valueInt:
		pi := (*[1 << 30]C.int)(ptr)[:l:l]
		pu := (*[1 << 30]C.unsigned)(ptr)[:l:l]
		switch a := v.Addr().Interface().(type) {
		case *[]int:
			*a = make([]int, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = int(pi[i])
			}
		case *[]uint:
			*a = make([]uint, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = uint(pu[i])
			}
		case *[]int16:
			*a = make([]int16, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = int16(pi[i])
			}
		case *[]uint16:
			*a = make([]uint16, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = uint16(pu[i])
			}
		case *[]int32:
			*a = make([]int32, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = int32(pi[i])
			}
		case *[]uint32:
			*a = make([]uint32, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = uint32(pu[i])
			}
		case *[]int8:
			*a = make([]int8, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = int8(pi[i])
			}
		case *[]uint8:
			*a = make([]uint8, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = uint8(pu[i])
			}
		case *[]bool:
			*a = make([]bool, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = bool(pi[i] != 0)
			}
		default:
			panic(fmt.Errorf("Can't set []int to []%s", v.Type().Elem().Kind().String()))
		}
	case valueInt64:
		switch a := v.Addr().Interface().(type) {
		case *[]int64:
			pi := (*[1 << 30]int64)(ptr)[:l:l]
			*a = make([]int64, cnt, cnt)
			copy(*a, pi)
		case *[]uint64:
			pi := (*[1 << 30]uint64)(ptr)[:l:l]
			*a = make([]uint64, cnt, cnt)
			copy(*a, pi)
		default:
			panic(fmt.Errorf("Can't set []uint to []%s", v.Type().Elem().Kind().String()))
		}
	case valueDouble:
		pi := (*[1 << 30]C.double)(ptr)[:l:l]
		switch a := v.Addr().Interface().(type) {
		case *[]float64:
			*a = make([]float64, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = float64(pi[i])
			}
		case *[]float32:
			*a = make([]float32, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = float32(pi[i])
			}
		default:
			panic(fmt.Errorf("Can't set []double to []%s", v.Type().Elem().Kind().String()))
		}
	case valueString:
		a := v.Addr().Interface().(*[]string)
		*a = make([]string, cnt, cnt)
		for i := 0; i < cnt; i++ {
			(*a)[i] = pl.getString(field, i+startIdx)
		}
	}
}

// Slow and generic method: convert c payload to go interface
// Use only for debug purposes
func (pl *payloadIface) getIface(field int) interface{} {

	if !pl.t.Fields[field].IsArray {
		switch pl.t.Fields[field].Type {
		case valueInt:
			return pl.getInt(field, 0)
		case valueInt64:
			return pl.getInt64(field, 0)
		case valueDouble:
			return pl.getFloat64(field, 0)
		case valueString:
			return pl.getString(field, 0)
		}
	}

	l := pl.getArrayLen(field)

	switch pl.t.Fields[field].Type {
	case valueInt:
		a := make([]int, l, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getInt(field, i)
		}
		return a
	case valueInt64:
		a := make([]int64, l, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getInt64(field, i)
		}
		return a
	case valueDouble:
		a := make([]float64, l, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getFloat64(field, i)
		}
		return a
	case valueString:
		a := make([]string, l, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getString(field, i)
		}
		return a
	}

	return nil
}

func (pl *payloadIface) getAsMap() map[string]interface{} {
	ret := make(map[string]interface{})

	for f := 1; f < len(pl.t.Fields); f++ {
		ret[pl.t.Fields[f].Name] = pl.getIface(f)
	}
	return ret
}
