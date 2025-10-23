package cjson

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/restream/reindexer/v5/bindings"
)

const (
	valueInt         = bindings.ValueInt
	valueBool        = bindings.ValueBool
	valueInt64       = bindings.ValueInt64
	valueDouble      = bindings.ValueDouble
	valueString      = bindings.ValueString
	valueUuid        = bindings.ValueUuid
	valueFloatVector = bindings.ValueFloatVector
	valueFloat       = bindings.ValueFloat
)

const (
	floatVectorDimensionOffset = 48
	floatVectorPtrMask         = (uint64(1) << floatVectorDimensionOffset) - uint64(1)
)

// to avoid gcc toolchain requirement
// types from C. Danger expectation about go struct packing is like C struct packing
type Cdouble float64
type Cfloat float32
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
	len Cuint
}

type LStringHeader struct {
	len  Cuint
	data [1]Cchar
}

const (
	kLargeJSONStrFlag byte = 0x80
)

func lengthLargeJsonString(p *byte) uint {
	ptr := uintptr(unsafe.Pointer(p))
	return uint(*(*byte)(unsafe.Pointer(ptr - 1))) |
		(uint(*(*byte)(unsafe.Pointer(ptr))) << 8) |
		(uint(*(*byte)(unsafe.Pointer(ptr + 1))) << 16) |
		((uint(*(*byte)(unsafe.Pointer(ptr + 2)) & (^kLargeJSONStrFlag))) << 24)
}

func lengthSmallJsonString(p *byte) uint {
	ptr := uintptr(unsafe.Pointer(p))
	return uint(*(*byte)(unsafe.Pointer(ptr))) |
		(uint(*(*byte)(unsafe.Pointer(ptr + 1))) << 8) |
		(uint(*(*byte)(unsafe.Pointer(ptr + 2))) << 16)
}

func jsonStringView(p *byte) []byte {
	ptr := uintptr(unsafe.Pointer(p))

	thirdByte := *(*byte)(unsafe.Pointer(ptr + 2))
	var strPtr uintptr
	var len uint
	if thirdByte&kLargeJSONStrFlag != 0 {
		len = lengthLargeJsonString(p)

		if unsafe.Sizeof(strPtr) == 4 {
			strPtr = uintptr(*(*byte)(unsafe.Pointer(ptr - 2))) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 3))) << 8) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 4))) << 16) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 5))) << 24)
		} else {
			strPtr = uintptr(*(*byte)(unsafe.Pointer(ptr - 2))) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 3))) << 8) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 4))) << 16) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 5))) << 24) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 6))) << 32) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 7))) << 40) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 8))) << 48) |
				(uintptr(*(*byte)(unsafe.Pointer(ptr - 9))) << 56)
		}
	} else {
		len = lengthSmallJsonString(p)
		strPtr = ptr - uintptr(len)
	}

	return (*[1 << 30]byte)(unsafe.Pointer(strPtr))[:len:len]
}

type payloadFieldType struct {
	Type                 int
	Name                 string
	Offset               uintptr
	Size                 uintptr
	FloatVectorDimension uint16
	IsArray              bool
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
		if fields[i].Type == valueFloatVector {
			fields[i].FloatVectorDimension = uint16(ser.GetVarUInt())
		} else {
			fields[i].FloatVectorDimension = 0
		}
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
		panic(fmt.Errorf("Null pointer dereference"))
	}

	f := &pl.t.Fields[field]

	if f.Type != typ {
		panic(fmt.Errorf("Invalid type cast of field '%s' from type '%d' to type '%d'", f.Name, f.Type, typ))
	}

	p := unsafe.Pointer(pl.p + uintptr(f.Offset))

	if !f.IsArray {
		if idx != 0 {
			panic(fmt.Errorf("Trying to access by index '%d' to non array field '%s'", idx, f.Name))
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

const hexChars = "0123456789abcdef"

func createUuid(v [2]uint64) string {
	buf := make([]byte, 36)
	buf[0] = hexChars[(v[0]>>60)&0xF]
	buf[1] = hexChars[(v[0]>>56)&0xF]
	buf[2] = hexChars[(v[0]>>52)&0xF]
	buf[3] = hexChars[(v[0]>>48)&0xF]
	buf[4] = hexChars[(v[0]>>44)&0xF]
	buf[5] = hexChars[(v[0]>>40)&0xF]
	buf[6] = hexChars[(v[0]>>36)&0xF]
	buf[7] = hexChars[(v[0]>>32)&0xF]
	buf[8] = '-'
	buf[9] = hexChars[(v[0]>>28)&0xF]
	buf[10] = hexChars[(v[0]>>24)&0xF]
	buf[11] = hexChars[(v[0]>>20)&0xF]
	buf[12] = hexChars[(v[0]>>16)&0xF]
	buf[13] = '-'
	buf[14] = hexChars[(v[0]>>12)&0xF]
	buf[15] = hexChars[(v[0]>>8)&0xF]
	buf[16] = hexChars[(v[0]>>4)&0xF]
	buf[17] = hexChars[v[0]&0xF]
	buf[18] = '-'
	buf[19] = hexChars[(v[1]>>60)&0xF]
	buf[20] = hexChars[(v[1]>>56)&0xF]
	buf[21] = hexChars[(v[1]>>52)&0xF]
	buf[22] = hexChars[(v[1]>>48)&0xF]
	buf[23] = '-'
	buf[24] = hexChars[(v[1]>>44)&0xF]
	buf[25] = hexChars[(v[1]>>40)&0xF]
	buf[26] = hexChars[(v[1]>>36)&0xF]
	buf[27] = hexChars[(v[1]>>32)&0xF]
	buf[28] = hexChars[(v[1]>>28)&0xF]
	buf[29] = hexChars[(v[1]>>24)&0xF]
	buf[30] = hexChars[(v[1]>>20)&0xF]
	buf[31] = hexChars[(v[1]>>16)&0xF]
	buf[32] = hexChars[(v[1]>>12)&0xF]
	buf[33] = hexChars[(v[1]>>8)&0xF]
	buf[34] = hexChars[(v[1]>>4)&0xF]
	buf[35] = hexChars[v[1]&0xF]
	return string(buf)
}

func (pl *payloadIface) getInt(field, idx int) int {
	p := pl.ptr(field, idx, valueInt)
	return int(*(*Cint)(p))
}

func (pl *payloadIface) getInt64(field, idx int) int64 {
	p := pl.ptr(field, idx, valueInt64)
	return *(*int64)(p)
}

func (pl *payloadIface) getUuid(field, idx int) string {
	p := pl.ptr(field, idx, valueUuid)
	return createUuid(*(*[2]uint64)(p))
}

func (pl *payloadIface) getFloatVector(field, idx int) []float32 {
	p := pl.ptr(field, idx, valueFloatVector)
	cFloatVectorView := *(*uint64)(p)
	ptr := uintptr(cFloatVectorView & floatVectorPtrMask)
	if ptr == 0 {
		return []float32{}
	} else {
		dim := cFloatVectorView >> floatVectorDimensionOffset
		return (*[1 << 30]float32)(unsafe.Pointer(ptr))[:dim:dim]
	}
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
const jsonSringType = 6

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
		hdrPtr := ppstring + pl.t.PStringHdrOffset
		strHdr := (*PStringHeader)(unsafe.Pointer(hdrPtr))
		dataPtr := unsafe.Pointer(hdrPtr + unsafe.Sizeof(PStringHeader{}))
		return (*[1 << 30]byte)(dataPtr)[:strHdr.len:strHdr.len]
	case jsonSringType:
		return jsonStringView((*uint8)(unsafe.Pointer(ppstring)))
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

// get c reflect value and set to go reflect value
func (pl *payloadIface) getValue(field int, idx int, v reflect.Value) {

	k := v.Type().Kind()
	if pl.t.Fields[field].Type != valueFloatVector {
		if k == reflect.Slice {
			el := reflect.New(v.Type().Elem()).Elem()
			extSlice := reflect.Append(v, el)
			v.Set(extSlice)
			v = v.Index(v.Len() - 1)
			k = v.Type().Kind()
		} else if k == reflect.Array {
			panic(fmt.Errorf("can not put single indexed value into the fixed size array"))
		}
	}
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
	case valueUuid:
		v.SetString(pl.getUuid(field, idx))
	case valueFloatVector:
		vec := pl.getFloatVector(field, idx)
		if k == reflect.Slice {
			extLen := v.Len() + len(vec)
			extSlice := reflect.MakeSlice(v.Type(), extLen, extLen)
			reflect.Copy(extSlice, v)
			offset := v.Len()
			for i := 0; i < len(vec); i++ {
				extSlice.Index(i + offset).SetFloat(float64(vec[i]))
			}
			v.Set(extSlice)
		} else if k == reflect.Array {
			if len(vec) > v.Len() {
				panic(fmt.Errorf("can not put float vector of size '%d' into array of size '%d", len(vec), v.Len()))
			}
			for i := 0; i < len(vec); i++ {
				v.Index(i).SetFloat(float64(vec[i]))
			}
		} else {
			panic(fmt.Errorf("can not put float vector value into not array field"))
		}
	default:
		panic(fmt.Errorf("unknown key value type '%d'", pl.t.Fields[field].Type))
	}
}

func (pl *payloadIface) getArray(field int, startIdx int, cnt int, v reflect.Value) {

	if cnt == 0 {
		slice := reflect.MakeSlice(v.Type(), 0, 0)
		v.Set(slice)
		return
	}

	ptr := pl.ptr(field, startIdx, pl.t.Fields[field].Type)
	l := pl.getArrayLen(field) - startIdx
	i := 0

	switch pl.t.Fields[field].Type {
	case valueInt:
		pi := (*[1 << 27]Cint)(ptr)[:cnt:cnt]
		pu := (*[1 << 27]Cunsigned)(ptr)[:cnt:cnt]
		switch a := v.Addr().Interface().(type) {
		case *[]int:
			if len(*a) == 0 {
				*a = make([]int, cnt)
			} else {
				i = len(*a)
				var tmp []int
				tmp, *a = *a, make([]int, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = int(pi[j])
			}
		case *[]uint:
			if len(*a) == 0 {
				*a = make([]uint, cnt)
			} else {
				i = len(*a)
				var tmp []uint
				tmp, *a = *a, make([]uint, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = uint(pu[j])
			}
		case *[]int16:
			if len(*a) == 0 {
				*a = make([]int16, cnt)
			} else {
				i = len(*a)
				var tmp []int16
				tmp, *a = *a, make([]int16, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = int16(pi[j])
			}
		case *[]uint16:
			if len(*a) == 0 {
				*a = make([]uint16, cnt)
			} else {
				i = len(*a)
				var tmp []uint16
				tmp, *a = *a, make([]uint16, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = uint16(pu[j])
			}
		case *[]int32:
			if len(*a) == 0 {
				*a = make([]int32, cnt)
			} else {
				i = len(*a)
				var tmp []int32
				tmp, *a = *a, make([]int32, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = int32(pi[j])
			}
		case *[]uint32:
			if len(*a) == 0 {
				*a = make([]uint32, cnt)
			} else {
				i = len(*a)
				var tmp []uint32
				tmp, *a = *a, make([]uint32, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = uint32(pu[j])
			}
		case *[]int8:
			if len(*a) == 0 {
				*a = make([]int8, cnt)
			} else {
				i = len(*a)
				var tmp []int8
				tmp, *a = *a, make([]int8, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = int8(pi[j])
			}
		case *[]uint8:
			if len(*a) == 0 {
				*a = make([]uint8, cnt)
			} else {
				i = len(*a)
				var tmp []uint8
				tmp, *a = *a, make([]uint8, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = uint8(pu[j])
			}
		case *[]bool:
			if len(*a) == 0 {
				*a = make([]bool, cnt)
			} else {
				i = len(*a)
				var tmp []bool
				tmp, *a = *a, make([]bool, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = bool(pi[j] != 0)
			}
		default:
			var slice reflect.Value
			if v.Len() == 0 {
				slice = reflect.MakeSlice(v.Type(), cnt, cnt)
			} else {
				i = v.Len()
				slice = reflect.Append(v, reflect.MakeSlice(v.Type(), cnt, cnt))
			}
			switch v.Type().Elem().Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				for j := 0; j < cnt; i, j = i+1, j+1 {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetUint(uint64(pu[j]))
						sv.Set(el)
					} else {
						sv.SetUint(uint64(pu[j]))
					}
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				for j := 0; j < cnt; i, j = i+1, j+1 {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetInt(int64(pi[j]))
						sv.Set(el)
					} else {
						sv.SetInt(int64(pi[j]))
					}
				}
			default:
				panic(fmt.Errorf("can not convert '[]%s' to '[]int'", v.Type().Elem().Kind().String()))
			}
			v.Set(slice)
		}
	case valueInt64:
		switch a := v.Addr().Interface().(type) {
		case *[]int64:
			pi := (*[1 << 27]int64)(ptr)[:cnt:cnt]
			if len(*a) == 0 {
				*a = make([]int64, cnt)
				copy(*a, pi)
			} else {
				*a = append(*a, pi...)
			}
		case *[]uint64:
			pi := (*[1 << 27]uint64)(ptr)[:cnt:cnt]
			if len(*a) == 0 {
				*a = make([]uint64, cnt)
				copy(*a, pi)
			} else {
				*a = append(*a, pi...)
			}
		case *[]int:
			pi := (*[1 << 27]int64)(ptr)[:cnt:cnt]
			i := len(*a)
			if i == 0 {
				*a = make([]int, cnt)
			} else {
				var tmp []int
				tmp, *a = *a, make([]int, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = int(pi[j])
			}
		case *[]uint:
			pi := (*[1 << 27]uint64)(ptr)[:cnt]
			i := len(*a)
			if i == 0 {
				*a = make([]uint, cnt)
			} else {
				var tmp []uint
				tmp, *a = *a, make([]uint, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = uint(pi[j])
			}
		default:
			var slice reflect.Value
			i := v.Len()
			if i == 0 {
				slice = reflect.MakeSlice(v.Type(), cnt, cnt)
			} else {
				slice = reflect.Append(v, reflect.MakeSlice(v.Type(), cnt, cnt))
			}
			switch v.Type().Elem().Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				pi := (*[1 << 27]uint64)(ptr)[:cnt:cnt]
				for j := 0; j < cnt; i, j = i+1, j+1 {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetUint(uint64(pi[j]))
						sv.Set(el)
					} else {
						sv.SetUint(uint64(pi[j]))
					}
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				pi := (*[1 << 27]int64)(ptr)[:cnt:cnt]
				for j := 0; j < cnt; i, j = i+1, j+1 {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetInt(int64(pi[j]))
						sv.Set(el)
					} else {
						sv.SetInt(int64(pi[j]))
					}
				}
			default:
				panic(fmt.Errorf("can not convert '[]%s' to '[]int64'", v.Type().Elem().Kind().String()))
			}
			v.Set(slice)
		}
	case valueDouble:
		pi := (*[1 << 27]Cdouble)(ptr)[:cnt:cnt]
		if v.Kind() == reflect.Array {
			if v.Len() < cnt {
				panic(fmt.Errorf("can not set %d values to array of %d elements", cnt, v.Len()))
			}
			switch v.Type().Elem().Kind() {
			case reflect.Float32:
				for i = 0; i < cnt; i++ {
					v.Index(i).SetFloat(float64(float32(pi[i])))
				}
			case reflect.Float64:
				for i = 0; i < cnt; i++ {
					v.Index(i).SetFloat(float64(pi[i]))
				}
			default:
				panic(fmt.Errorf("can not convert '[]%s' to '[]double'", v.Type().Elem().Kind().String()))
			}
		} else {
			switch a := v.Addr().Interface().(type) {
			case *[]float64:
				if len(*a) == 0 {
					*a = make([]float64, cnt)
				} else {
					i = len(*a)
					var tmp []float64
					tmp, *a = *a, make([]float64, len(*a)+cnt)
					copy(*a, tmp)
				}
				for j := 0; j < cnt; i, j = i+1, j+1 {
					(*a)[i] = float64(pi[j])
				}
			case *[]float32:
				if len(*a) == 0 {
					*a = make([]float32, cnt)
				} else {
					i = len(*a)
					var tmp []float32
					tmp, *a = *a, make([]float32, len(*a)+cnt)
					copy(*a, tmp)
				}
				for j := 0; j < cnt; i, j = i+1, j+1 {
					(*a)[i] = float32(pi[j])
				}
			default:
				var slice reflect.Value
				if v.Len() == 0 {
					slice = reflect.MakeSlice(v.Type(), cnt, cnt)
				} else {
					i = v.Len()
					slice = reflect.Append(v, reflect.MakeSlice(v.Type(), cnt, cnt))
				}
				for j := 0; j < cnt; i, j = i+1, j+1 {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetFloat(float64(pi[j]))
						sv.Set(el)
					} else {
						sv.SetFloat(float64(pi[j]))
					}
				}
				v.Set(slice)
			}
		}
	case valueFloat:
		pi := (*[1 << 27]Cfloat)(ptr)[:cnt:cnt]
		if v.Kind() == reflect.Array {
			if v.Len() < cnt {
				panic(fmt.Errorf("can not set %d values to array of %d elements", cnt, v.Len()))
			}
			switch v.Type().Elem().Kind() {
			case reflect.Float32:
				for i = 0; i < cnt; i++ {
					v.Index(i).SetFloat(float64(float32(pi[i])))
				}
			case reflect.Float64:
				for i = 0; i < cnt; i++ {
					v.Index(i).SetFloat(float64(float32(pi[i])))
				}
			default:
				panic(fmt.Errorf("can not convert '[]%s' to '[]float32'", v.Type().Elem().Kind().String()))
			}
		} else {
			switch a := v.Addr().Interface().(type) {
			case *[]float64:
				if len(*a) == 0 {
					*a = make([]float64, cnt)
				} else {
					i = len(*a)
					var tmp []float64
					tmp, *a = *a, make([]float64, len(*a)+cnt)
					copy(*a, tmp)
				}
				for j := 0; j < cnt; i, j = i+1, j+1 {
					(*a)[i] = float64(float32(pi[j]))
				}
			case *[]float32:
				if len(*a) == 0 {
					*a = make([]float32, cnt)
				} else {
					i = len(*a)
					var tmp []float32
					tmp, *a = *a, make([]float32, len(*a)+cnt)
					copy(*a, tmp)
				}
				for j := 0; j < cnt; i, j = i+1, j+1 {
					(*a)[i] = float32(pi[j])
				}
			default:
				var slice reflect.Value
				if v.Len() == 0 {
					slice = reflect.MakeSlice(v.Type(), cnt, cnt)
				} else {
					i = v.Len()
					slice = reflect.Append(v, reflect.MakeSlice(v.Type(), cnt, cnt))
				}
				for j := 0; j < cnt; i, j = i+1, j+1 {
					sv := slice.Index(i)
					if sv.Type().Kind() == reflect.Ptr {
						el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
						el.Elem().SetFloat(float64(float32(pi[j])))
						sv.Set(el)
					} else {
						sv.SetFloat(float64(float32(pi[j])))
					}
				}
				v.Set(slice)
			}
		}
	case valueBool:
		pb := (*[1 << 27]Cbool)(ptr)[:cnt:cnt]
		switch a := v.Addr().Interface().(type) {
		case *[]bool:
			if len(*a) == 0 {
				*a = make([]bool, cnt)
			} else {
				i = len(*a)
				var tmp []bool
				tmp, *a = *a, make([]bool, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = bool(pb[j] != 0)
			}
		default:
			var slice reflect.Value
			if v.Len() == 0 {
				slice = reflect.MakeSlice(v.Type(), cnt, cnt)
			} else {
				i = v.Len()
				slice = reflect.Append(v, reflect.MakeSlice(v.Type(), cnt, cnt))
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				sv := slice.Index(i)
				if sv.Type().Kind() == reflect.Ptr {
					el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
					el.Elem().SetBool(bool(pb[j] != 0))
					sv.Set(el)
				} else {
					sv.SetBool(bool(pb[j] != 0))
				}
			}
			v.Set(slice)
		}
	case valueString:
		if a, ok := v.Addr().Interface().(*[]string); ok {
			if len(*a) == 0 {
				*a = make([]string, cnt)
			} else {
				i = len(*a)
				var tmp []string
				tmp, *a = *a, make([]string, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = pl.getString(field, j+startIdx)
			}
		} else {
			var slice reflect.Value
			if v.Len() == 0 {
				slice = reflect.MakeSlice(v.Type(), cnt, cnt)
			} else {
				i = v.Len()
				slice = reflect.Append(v, reflect.MakeSlice(v.Type(), cnt, cnt))
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				s := pl.getString(field, j+startIdx)
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
	case valueUuid:
		pi := (*[1 << 27]uint64)(ptr)[: l*2 : l*2]
		if a, ok := v.Addr().Interface().(*[]string); ok {
			if len(*a) == 0 {
				*a = make([]string, cnt)
			} else {
				i = len(*a)
				var tmp []string
				tmp, *a = *a, make([]string, len(*a)+cnt)
				copy(*a, tmp)
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				(*a)[i] = createUuid([2]uint64{pi[j*2], pi[j*2+1]})
			}
		} else {
			var slice reflect.Value
			if v.Len() == 0 {
				slice = reflect.MakeSlice(v.Type(), cnt, cnt)
			} else {
				i = v.Len()
				slice = reflect.Append(v, reflect.MakeSlice(v.Type(), cnt, cnt))
			}
			for j := 0; j < cnt; i, j = i+1, j+1 {
				sv := slice.Index(i)
				if sv.Type().Kind() == reflect.Ptr {
					el := reflect.New(reflect.New(sv.Type().Elem()).Elem().Type())
					el.Elem().SetString(createUuid([2]uint64{pi[j*2], pi[j*2+1]}))
					sv.Set(el)
				} else {
					sv.SetString(createUuid([2]uint64{pi[j*2], pi[j*2+1]}))
				}
			}
			v.Set(slice)
		}
	case valueFloatVector:
		panic(fmt.Errorf("array of float vector is not supported"))
	default:
		panic(fmt.Errorf("got C array with elements of unknown C type %d in field '%s' for go type '%s'", pl.t.Fields[field].Type, pl.t.Fields[field].Name, v.Type().Elem().Kind().String()))
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
		case valueUuid:
			return pl.getUuid(field, 0)
		case valueFloatVector:
			return pl.getFloatVector(field, 0)
		}
	}

	l := pl.getArrayLen(field)

	switch pl.t.Fields[field].Type {
	case valueInt:
		a := make([]int, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getInt(field, i)
		}
		return a
	case valueInt64:
		a := make([]int64, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getInt64(field, i)
		}
		return a
	case valueDouble:
		a := make([]float64, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getFloat64(field, i)
		}
		return a
	case valueFloat:
		panic(fmt.Errorf("float32 can not be indexed"))
	case valueString:
		a := make([]string, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getString(field, i)
		}
		return a
	case valueUuid:
		a := make([]string, l)
		for i := 0; i < l; i++ {
			a[i] = pl.getUuid(field, i)
		}
		return a
	case valueFloatVector:
		panic(fmt.Errorf("array of float vector is not supported"))
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
