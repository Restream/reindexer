package cjson

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/restream/reindexer/bindings"
)

const (
	valueInt    = bindings.ValueInt
	valueBool   = bindings.ValueBool
	valueInt64  = bindings.ValueInt64
	valueDouble = bindings.ValueDouble
	valueString = bindings.ValueString
)

// to avoid gcc toolchain requirement
// types from C. Danger expectation about go struct packing is like C struct packing
type Cdouble float64
type Cint int32
type Cuint uint32
type Cunsigned uint32
type Cbool int8
type Cchar int8

type ArrayHeader struct {
	offset Cunsigned
	len    Cint
}

type PStringHeader struct {
	cstr unsafe.Pointer
	len  Cint
}

type LStringHeader struct {
	len  Cuint
	data [1]Cchar
}

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

func (pt *payloadType) Read(ser *Serializer, skip bool) {
	pt.PStringHdrOffset = uintptr(ser.GetVarUInt())
	fieldsCount := int(ser.GetVarUInt())
	fields := make([]payloadFieldType, fieldsCount, fieldsCount)

	for i := 0; i < fieldsCount; i++ {
		fields[i].Type = int(ser.GetVarUInt())
		fields[i].Name = ser.GetVString()
		fields[i].Offset = uintptr(ser.GetVarUInt())
		fields[i].Size = uintptr(ser.GetVarUInt())
		fields[i].IsArray = ser.GetVarUInt() != 0
		jsonPathCnt := ser.GetVarUInt()
		for ; jsonPathCnt != 0; jsonPathCnt-- {
			ser.GetVString()
		}
	}
	if !skip {
		pt.Fields = fields
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
	arr := (*ArrayHeader)(p)
	if idx >= int(arr.len) {
		panic(fmt.Errorf("Index %d is out of bound %d on array field '%s'", idx, int(arr.len), f.Name))
	}
	return unsafe.Pointer(pl.p + uintptr(arr.offset) + uintptr(idx)*f.Size)
}

func (pl *payloadIface) getInt(field, idx int) int {
	p := pl.ptr(field, idx, valueInt)
	return int(*(*Cint)(p))
}

func (pl *payloadIface) getInt64(field, idx int) int64 {
	p := pl.ptr(field, idx, valueInt64)
	return *(*int64)(p)
}

func (pl *payloadIface) getFloat64(field, idx int) float64 {
	p := pl.ptr(field, idx, valueDouble)
	return float64(*(*Cdouble)(p))
}

func (pl *payloadIface) getBool(field, idx int) bool {
	p := pl.ptr(field, idx, valueBool)
	return bool(*(*Cbool)(p) != 0)
}

const tagShift = 59
const tagMask = uint64(7) << tagShift
const lStringType = 1
const keySringType = 5

func (pl *payloadIface) getBytes(field, idx int) []byte {
	p := pl.ptr(field, idx, valueString)
	// p is pointer to p_string. see core/keyvalue/p_string.h

	psType := (*(*uint64)(p) & tagMask) >> tagShift
	ppstring := uintptr(*(*uint64)(p) & ^tagMask)
	switch psType {
	case lStringType:
		strHdr := (*LStringHeader)(unsafe.Pointer(ppstring))
		return (*[1 << 30]byte)(unsafe.Pointer(&strHdr.data))[:strHdr.len:strHdr.len]
	case keySringType:
		strHdr := (*PStringHeader)(unsafe.Pointer(ppstring + pl.t.PStringHdrOffset))
		return (*[1 << 30]byte)(strHdr.cstr)[:strHdr.len:strHdr.len]
	default:
		panic(fmt.Sprintf("Unknow string type in payload value: %d", psType))
	}
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
	return int((*ArrayHeader)(p).len)
}

// get c reflect value and set to go reflect valie
func (pl *payloadIface) getValue(field int, idx int, v reflect.Value) {

	k := v.Type().Kind()
	switch pl.t.Fields[field].Type {
	case valueBool:
		v.SetBool(pl.getBool(field, idx))
	case valueInt:
		switch k {
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
		pi := (*[1 << 27]Cint)(ptr)[:l:l]
		pu := (*[1 << 27]Cunsigned)(ptr)[:l:l]
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
			slice := reflect.MakeSlice(v.Type(), cnt, cnt)
			switch v.Type().Elem().Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				for i := 0; i < cnt; i++ {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetUint(uint64(pu[i]))
						sv.Set(el)
					} else {
						sv.SetUint(uint64(pu[i]))
					}
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				for i := 0; i < cnt; i++ {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetInt(int64(pi[i]))
						sv.Set(el)
					} else {
						sv.SetInt(int64(pi[i]))
					}
				}
			default:
				panic(fmt.Errorf("Can't set []int to []%s", v.Type().Elem().Kind().String()))
			}
			v.Set(slice)
		}
	case valueInt64:
		switch a := v.Addr().Interface().(type) {
		case *[]int64:
			pi := (*[1 << 27]int64)(ptr)[:l:l]
			*a = make([]int64, cnt, cnt)
			copy(*a, pi)
		case *[]uint64:
			pi := (*[1 << 27]uint64)(ptr)[:l:l]
			*a = make([]uint64, cnt, cnt)
			copy(*a, pi)
		case *[]int:
			pi := (*[1 << 27]int64)(ptr)[:l:l]
			*a = make([]int, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = int(pi[i])
			}
		case *[]uint:
			pi := (*[1 << 27]uint64)(ptr)[:l:l]
			*a = make([]uint, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = uint(pi[i])
			}
		default:
			slice := reflect.MakeSlice(v.Type(), cnt, cnt)
			switch v.Type().Elem().Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				pi := (*[1 << 27]uint64)(ptr)[:l:l]
				for i := 0; i < cnt; i++ {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetUint(uint64(pi[i]))
						sv.Set(el)
					} else {
						sv.SetUint(uint64(pi[i]))
					}
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				pi := (*[1 << 27]int64)(ptr)[:l:l]
				for i := 0; i < cnt; i++ {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetInt(int64(pi[i]))
						sv.Set(el)
					} else {
						sv.SetInt(int64(pi[i]))
					}
				}
			default:
				panic(fmt.Errorf("Can't set []int64 to []%s", v.Type().Elem().Kind().String()))
			}
			v.Set(slice)
		}
	case valueDouble:
		pi := (*[1 << 27]Cdouble)(ptr)[:l:l]
		if v.Kind() == reflect.Array {
			if v.Len() < cnt {
				panic(fmt.Errorf("Can't set %d values to array of %d elements", cnt, v.Len()))
			}
			switch v.Type().Elem().Kind() {
			case reflect.Float32:
				for i := 0; i < cnt; i++ {
					v.Index(i).SetFloat(float64(float32(pi[i])))
				}
			case reflect.Float64:
				for i := 0; i < cnt; i++ {
					v.Index(i).SetFloat(float64(pi[i]))
				}
			default:
				panic(fmt.Errorf("Can't set []double to []%s", v.Type().Elem().Kind().String()))
			}
		} else {
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
				slice := reflect.MakeSlice(v.Type(), cnt, cnt)
				for i := 0; i < cnt; i++ {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetFloat(float64(pi[i]))
						sv.Set(el)
					} else {
						sv.SetFloat(float64(pi[i]))
					}
				}
				v.Set(slice)
			}
		}
	case valueBool:
		pb := (*[1 << 27]Cbool)(ptr)[:l:l]
		switch a := v.Addr().Interface().(type) {
		case *[]bool:
			*a = make([]bool, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = bool(pb[i] != 0)
			}
		default:
			slice := reflect.MakeSlice(v.Type(), cnt, cnt)
			for i := 0; i < cnt; i++ {
				sv := slice.Index(i)
				if sv.Type().Kind() == reflect.Ptr {
					el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
					el.Elem().SetBool(bool(pb[i] != 0))
					sv.Set(el)
				} else {
					sv.SetBool(bool(pb[i] != 0))
				}
			}
			v.Set(slice)
		}
	case valueString:
		if a, ok := v.Addr().Interface().(*[]string); ok {
			*a = make([]string, cnt, cnt)
			for i := 0; i < cnt; i++ {
				(*a)[i] = pl.getString(field, i+startIdx)
			}
		} else {
			slice := reflect.MakeSlice(v.Type(), cnt, cnt)
			for i := 0; i < cnt; i++ {
				s := pl.getString(field, i+startIdx)
				sv := slice.Index(i)
				if sv.Type().Kind() == reflect.Ptr {
					el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
					el.Elem().SetString(s)
					sv.Set(el)
				} else {
					sv.SetString(s)
				}
			}
			v.Set(slice)
		}
	default:
		panic(fmt.Errorf("Got C array with elements of unknown C type %d in field '%s' for go type '%s'", pl.t.Fields[field].Type, pl.t.Fields[field].Name, v.Type().Elem().Kind().String()))
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
