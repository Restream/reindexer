package cjson

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/restream/reindexer/v5/bindings"
)

var (
	ifaceSlice     []interface{}
	ifaceSliceType = reflect.TypeOf(ifaceSlice)
)

type LoggerOwner interface {
	GetLogger() bindings.Logger
}

type Decoder struct {
	ser         *Serializer
	state       *State
	ctagsCache  *ctagsCache
	loggerOwner LoggerOwner
}

const MaxIndexes = 256

func fieldByTag(t reflect.Type, tag string) (result reflect.StructField, ok bool) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	for i := 0; i < t.NumField(); i++ {
		result = t.Field(i)
		if ftag, _ := splitStr(result.Tag.Get("json"), ','); len(ftag) > 0 {
			if tag == ftag {
				return result, true
			}
		} else if result.Anonymous {
			if result, ok := fieldByTag(result.Type, tag); ok {
				result.Index = append([]int{i}, result.Index...)
				return result, true
			}
		} else if result.Name == tag {
			return result, true
		}
	}
	return reflect.StructField{}, false
}

func createEmbedByIdx(v reflect.Value, idx []int) {

	v = reflect.Indirect(v).Field(idx[0])

	if v.Kind() == reflect.Ptr && v.IsNil() {
		v.Set(reflect.New(v.Type().Elem()))
	}
	if len(idx) > 2 {
		createEmbedByIdx(v, idx[1:])
	}
}

func (dec *Decoder) skipStruct(pl *payloadIface, rdser *Serializer, fieldsoutcnt []int, tag ctag) bool {
	ctagType := tag.Type()

	if ctagType == TAG_END {
		return false
	}
	ctagField := tag.Field()

	//fmt.Printf("skipping '%s' %s\n", dec.state.tagsMatcher.tag2name(tag.Name()), tag.Dump())

	if ctagField >= 0 {
		cnt := &fieldsoutcnt[ctagField]
		switch ctagType {
		case TAG_ARRAY:
			count := int(rdser.GetVarUInt())
			*cnt += count
		default:
			(*cnt)++
		}
	} else {
		switch ctagType {
		case TAG_OBJECT:
			for dec.skipStruct(pl, rdser, fieldsoutcnt, rdser.GetCTag()) {
			}
		case TAG_ARRAY:
			atag := rdser.GetCArrayTag()
			count := atag.Count()
			subtag := atag.Tag()
			for i := 0; i < count; i++ {
				switch subtag {
				case TAG_OBJECT:
					dec.skipStruct(pl, rdser, fieldsoutcnt, rdser.GetCTag())
				default:
					skipTag(rdser, subtag)
				}
			}
		default:
			skipTag(rdser, ctagType)
		}
	}
	return true
}
func skipTag(rdser *Serializer, tagType int16) {
	switch tagType {
	case TAG_DOUBLE:
		rdser.GetDouble()
	case TAG_VARINT:
		rdser.GetVarInt()
	case TAG_BOOL:
		rdser.GetVarUInt()
	case TAG_NULL:
	case TAG_STRING:
		rdser.GetVString()
	case TAG_UUID:
		rdser.GetUuid()
	case TAG_FLOAT:
		rdser.GetFloat32()
	default:
		panic(fmt.Errorf("can not skip tagType '%s'", tagTypeName(tagType)))
	}
}

func ctagTypeName(tagType int16) string {
	switch tagType {
	case TAG_VARINT:
		return "int"
	case TAG_DOUBLE:
		return "double"
	case TAG_STRING:
		return "string"
	case TAG_BOOL:
		return "bool"
	case TAG_NULL:
		return "null"
	case TAG_ARRAY:
		return "array"
	case TAG_OBJECT:
		return "object"
	case TAG_END:
		return "end"
	case TAG_UUID:
		return "uuid"
	case TAG_FLOAT:
		return "float"
	default:
		panic(fmt.Errorf("unknown ctag type '%d'", tagType))
	}
}

func asInt(rdser *Serializer, tagType int16) int64 {
	switch tagType {
	case TAG_BOOL:
		return int64(rdser.GetVarUInt())
	case TAG_VARINT:
		return rdser.GetVarInt()
	case TAG_DOUBLE:
		return int64(rdser.GetDouble())
	case TAG_FLOAT:
		return int64(rdser.GetFloat32())
	default:
		panic(fmt.Errorf("can not convert tagType '%s' to 'int'", tagTypeName(tagType)))
	}
}

func asFloat(rdser *Serializer, tagType int16) float64 {
	switch tagType {
	case TAG_VARINT:
		return float64(rdser.GetVarInt())
	case TAG_DOUBLE:
		return rdser.GetDouble()
	case TAG_FLOAT:
		return float64(rdser.GetFloat32())
	default:
		panic(fmt.Errorf("can not convert tagType '%s' to 'float'", tagTypeName(tagType)))
	}
}

func asString(rdser *Serializer, tagType int16) string {
	switch tagType {
	case TAG_STRING:
		return rdser.GetVString()
	case TAG_UUID:
		return rdser.GetUuid()
	default:
		panic(fmt.Errorf("can not convert tagType '%s' to 'string'", tagTypeName(tagType)))
	}
}

const maxInt = int(^uint(0) >> 1)
const minInt = -(maxInt - 1)

func asIface(rdser *Serializer, tagType int16) interface{} {
	switch tagType {
	case TAG_VARINT:
		v := rdser.GetVarInt()
		if v < int64(minInt) || v > int64(maxInt) {
			return v
		} else {
			return int(v)
		}
	case TAG_DOUBLE:
		return rdser.GetDouble()
	case TAG_BOOL:
		return rdser.GetVarInt() != 0
	case TAG_STRING:
		return rdser.GetVString()
	case TAG_NULL:
		return nil
	case TAG_UUID:
		return rdser.GetUuid()
	case TAG_FLOAT:
		return rdser.GetFloat32()
	default:
		panic(fmt.Errorf("can not convert tagType '%s' to 'interface'", tagTypeName(tagType)))
	}
}

func mkSlice(v *reflect.Value, count int) (offset int) {
	offset = v.Len()
	switch a := v.Addr().Interface().(type) {
	case *[]string:
		if offset == 0 {
			*a = make([]string, count)
		} else {
			*a = append(*a, make([]string, count)...)
		}
	case *[]int:
		if offset == 0 {
			*a = make([]int, count)
		} else {
			*a = append(*a, make([]int, count)...)
		}
	case *[]int64:
		if offset == 0 {
			*a = make([]int64, count)
		} else {
			*a = append(*a, make([]int64, count)...)
		}
	case *[]int32:
		if offset == 0 {
			*a = make([]int32, count)
		} else {
			*a = append(*a, make([]int32, count)...)
		}
	case *[]int16:
		if offset == 0 {
			*a = make([]int16, count)
		} else {
			*a = append(*a, make([]int16, count)...)
		}
	case *[]int8:
		if offset == 0 {
			*a = make([]int8, count)
		} else {
			*a = append(*a, make([]int8, count)...)
		}
	case *[]uint:
		if offset == 0 {
			*a = make([]uint, count)
		} else {
			*a = append(*a, make([]uint, count)...)
		}
	case *[]uint64:
		if offset == 0 {
			*a = make([]uint64, count)
		} else {
			*a = append(*a, make([]uint64, count)...)
		}
	case *[]uint32:
		if offset == 0 {
			*a = make([]uint32, count)
		} else {
			*a = append(*a, make([]uint32, count)...)
		}
	case *[]uint16:
		if offset == 0 {
			*a = make([]uint16, count)
		} else {
			*a = append(*a, make([]uint16, count)...)
		}
	case *[]uint8:
		if offset == 0 {
			*a = make([]uint8, count)
		} else {
			*a = append(*a, make([]uint8, count)...)
		}
	case *[]float64:
		if offset == 0 {
			*a = make([]float64, count)
		} else {
			*a = append(*a, make([]float64, count)...)
		}
	case *[]float32:
		if offset == 0 {
			*a = make([]float32, count)
		} else {
			*a = append(*a, make([]float32, count)...)
		}
	case *[]bool:
		if offset == 0 {
			*a = make([]bool, count)
		} else {
			*a = append(*a, make([]bool, count)...)
		}
	default:
		if offset == 0 {
			v.Set(reflect.MakeSlice(v.Type(), count, count))
		} else {
			v.Set(reflect.AppendSlice(*v, reflect.MakeSlice(v.Type(), count, count)))
		}
	}
	return
}

func mkValue(ctagType int16) (v reflect.Value) {
	switch ctagType {
	case TAG_STRING, TAG_UUID:
		v = reflect.New(reflect.TypeOf("")).Elem()
	case TAG_VARINT:
		v = reflect.New(reflect.TypeOf(0)).Elem()
	case TAG_DOUBLE:
		v = reflect.New(reflect.TypeOf(0.0)).Elem()
	case TAG_BOOL:
		v = reflect.New(reflect.TypeOf(false)).Elem()
	case TAG_OBJECT:
		v = reflect.New(reflect.TypeOf(make(map[string]interface{}))).Elem()
	case TAG_ARRAY:
		v = reflect.New(ifaceSliceType).Elem()
	case TAG_FLOAT:
		v = reflect.New(reflect.TypeOf(float32(0.0))).Elem()
	default:
		panic(fmt.Errorf("invalid ctagType=%d", ctagType))
	}
	return v
}

func (dec *Decoder) decodeSlice(pl *payloadIface, rdser *Serializer, v *reflect.Value, fieldsoutcnt []int, cctagsPath []int16) {
	atag := rdser.GetCArrayTag()
	count := atag.Count()
	subtag := atag.Tag()

	var origV reflect.Value
	var ptr unsafe.Pointer

	k := v.Kind()

	offset := 0
	switch k {
	case reflect.Slice:
		offset = mkSlice(v, count)
		ptr = unsafe.Pointer(v.Pointer())
	case reflect.Interface:
		origV = *v
		*v = reflect.ValueOf(reflect.New(ifaceSliceType).Interface()).Elem()
		offset = mkSlice(v, count)
		ptr = unsafe.Pointer(v.Pointer())
	case reflect.Array:
		if v.Len() < count {
			panic(fmt.Errorf("array bounds overflow. Required %d, but array len is %d", count, v.Len()))
		}
		ptr = unsafe.Pointer(v.Index(0).Addr().Pointer())
		// offset is 0
		// No concatenation for the fixed size arrays
	default:
		if count == 0 { // Allows empty slice for any scalar type (using default value)
			return
		} else {
			panic(fmt.Errorf("can not convert '%s' to 'array'", v.Type().Kind().String()))
		}
	}

	if subtag != TAG_OBJECT {
		k := v.Type().Elem().Kind()
		isPtr := false
		if k == reflect.Ptr {
			k = v.Type().Elem().Elem().Kind()
			isPtr = true
		}
		switch k {
		case reflect.Int:
			if !isPtr {
				sl := (*[1 << 28]int)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = int(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*int)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := int(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Uint:
			if !isPtr {
				sl := (*[1 << 28]uint)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = uint(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*uint)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := uint(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Int64:
			if !isPtr {
				sl := (*[1 << 27]int64)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = int64(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*int64)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := int64(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Uint64:
			if !isPtr {
				sl := (*[1 << 27]uint64)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = uint64(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*uint64)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := uint64(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Int32:
			if !isPtr {
				sl := (*[1 << 28]int32)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = int32(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*int32)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := int32(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Uint32:
			if !isPtr {
				sl := (*[1 << 28]uint32)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = uint32(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*uint32)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := uint32(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Int16:
			if !isPtr {
				sl := (*[1 << 29]int16)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = int16(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*int16)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := int16(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Uint16:
			if !isPtr {
				sl := (*[1 << 29]uint16)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = uint16(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*uint16)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := uint16(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Int8:
			if !isPtr {
				sl := (*[1 << 30]int8)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = int8(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*int8)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := int8(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Uint8:
			if !isPtr {
				sl := (*[1 << 30]uint8)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = uint8(asInt(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*uint8)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					u := uint8(asInt(rdser, subtag))
					sl[i] = &u
				}
			}
		case reflect.Float32:
			if !isPtr {
				sl := (*[1 << 28]float32)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = float32(asFloat(rdser, subtag))
				}
			} else {
				sl := (*[1 << 28]*float32)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					f := float32(asFloat(rdser, subtag))
					sl[i] = &f
				}
			}
		case reflect.Float64:
			if !isPtr {
				sl := (*[1 << 27]float64)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = asFloat(rdser, subtag)
				}
			} else {
				sl := (*[1 << 28]*float64)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					f := asFloat(rdser, subtag)
					sl[i] = &f
				}
			}
		case reflect.Bool:
			if !isPtr {
				sl := (*[1 << 27]bool)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = rdser.GetVarUInt() != 0
				}
			} else {
				sl := (*[1 << 28]*bool)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					b := rdser.GetVarUInt() != 0
					sl[i] = &b
				}
			}
		case reflect.String:
			if !isPtr {
				sl := (*[1 << 27]string)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					sl[i] = asString(rdser, subtag)
				}
			} else {
				sl := (*[1 << 28]*string)(ptr)[offset : offset+count : offset+count]
				for i := 0; i < count; i++ {
					s := asString(rdser, subtag)
					sl[i] = &s
				}
			}
		case reflect.Interface:
			sl := (*[1 << 27]interface{})(ptr)[offset : offset+count : offset+count]
			for i := 0; i < count; i++ {
				sl[i] = asIface(rdser, subtag)
			}
		default:
			panic(fmt.Errorf("internal error - can't decode array of type %s", v.Type().Elem().Kind().String()))
		}
	} else {
		for i := offset; i < offset+count; i++ {
			dec.decodeValue(pl, rdser, v.Index(i), fieldsoutcnt, cctagsPath)
		}
	}
	if k == reflect.Interface {
		origV.Set(*v)
		*v = origV
	}
}

func (dec *Decoder) tryToAddNewTag(cctagsPathStr string, cctagsPath []int16, v reflect.Value, name string, idx **[]int) {
	dec.state.lock.Lock()
	defer dec.state.lock.Unlock()
	if *idx == nil {
		*idx = dec.ctagsCache.FindOrAdd(cctagsPath)
	}
	if len(**idx) == 0 {
		if sf, ok := fieldByTag(v.Type(), name); ok {
			**idx = sf.Index
		} else {
			// Add tags path to the missing fields cache to avoid unique lock for those fields later
			dec.ctagsCache.missing[cctagsPathStr] = struct{}{}
		}
	}
}

func (dec *Decoder) ResetSerializer() {
	if dec.ser == nil {
		dec.ser = NewPoolSerializer()
	} else {
		dec.ser.Reset()
	}
}

func (dec *Decoder) decodeValue(pl *payloadIface, rdser *Serializer, v reflect.Value, fieldsoutcnt []int, cctagsPath []int16) bool {

	ctag := rdser.GetCTag()
	ctagType := ctag.Type()

	switch ctagType {
	case TAG_END:
		return false
	case TAG_NULL:
		return true
	}

	ctagField := ctag.Field()
	ctagName := ctag.Name()

	k := v.Kind()
	initialV := v
	if k == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
		k = v.Kind()
	}
	var idx *[]int

	mv, isMap := v, false
	if ctagName != 0 {
		cctagsPath = append(cctagsPath, ctagName)
		if k == reflect.Interface || (k == reflect.Map && v.Type().Elem().Kind() == reflect.Interface) {
			if v.IsNil() {
				v.Set(reflect.ValueOf(make(map[string]interface{})))
			}
			mv = reflect.ValueOf(v.Interface())
			v, isMap = mkValue(ctagType), true
		} else if k == reflect.Map {
			if v.IsNil() {
				v.Set(reflect.MakeMap(v.Type()))
			}
			mv = reflect.ValueOf(v.Interface())
			v, isMap = reflect.New(v.Type().Elem()).Elem(), true
		} else if k == reflect.Struct {

			// try to find in cache in RO mode
			idx = dec.ctagsCache.Find(cctagsPath)

			if idx == nil || len(*idx) == 0 {
				dec.ResetSerializer()
				dec.ser.WriteInts16(cctagsPath)
				cctagsPathStr := string(dec.ser.Bytes())
				if _, ok := dec.ctagsCache.missing[cctagsPathStr]; ok {
					// Tags path does not exists in go struct
					return dec.skipStruct(pl, rdser, fieldsoutcnt, ctag)
				} else {
					//  Tags path was not found in cache
					name := dec.state.tagsMatcher.tag2name(ctagName)
					// lock must be upgraded to exclusive here to add tag
					dec.state.lock.RUnlock()
					dec.tryToAddNewTag(cctagsPathStr, cctagsPath, v, name, &idx)
					dec.state.lock.RLock()
				}
			}
			if len(*idx) != 0 {
				if len(*idx) > 1 {
					createEmbedByIdx(v, *idx)
					v = v.FieldByIndex(*idx)
				} else {
					v = v.Field((*idx)[0])
				}
			} else {
				return dec.skipStruct(pl, rdser, fieldsoutcnt, ctag)
			}
		} else {
			panic(fmt.Errorf("err: intf=%s, name='%s' %s", v.Type().Name(), dec.state.tagsMatcher.tag2name(ctagName), ctag.Dump()))
		}
		initialV = v
		k = v.Kind()
		if k == reflect.Ptr {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
			k = v.Kind()
		}
	}

	//fmt.Printf("intf=%s, name='%s' %s,tagspath=%v,idx=%v\n", v.Type().Name(), dec.state.tagsMatcher.tag2name(ctagName), ctag.Dump(), cctagsPath, *idx)

	if ctagField >= 0 {
		// get data from payload object
		cnt := &fieldsoutcnt[ctagField]
		field := int(ctagField)
		switch ctagType {
		case TAG_ARRAY:
			count := int(rdser.GetVarUInt())
			if k == reflect.Slice || k == reflect.Array || count != 0 { // Allows empty slice for any scalar type (using default value)
				if pl.t.Fields[field].Type == bindings.ValueFloatVector {
					pl.getValue(field, *cnt, v)
					(*cnt)++
				} else {
					pl.getArray(field, *cnt, count, v)
					*cnt += count
				}
			} else {
				initialV.Set(reflect.Zero(initialV.Type())) // Set nil to scalar pointers, intialized with empty arrays
			}
		default:
			pl.getValue(field, *cnt, v)
			(*cnt)++
		}
	} else {
		// get data from serialized tuple
		switch ctagType {
		case TAG_ARRAY:
			dec.decodeSlice(pl, rdser, &v, fieldsoutcnt, cctagsPath)
			if v.Kind() != reflect.Array && v.Kind() != reflect.Slice && v.Kind() != reflect.Interface {
				initialV.Set(reflect.Zero(initialV.Type())) // Set nil to scalar pointers, intialized with empty arrays
			}
		case TAG_OBJECT:
			for dec.decodeValue(pl, rdser, v, fieldsoutcnt, cctagsPath) {
			}
		case TAG_STRING, TAG_UUID:
			var str string
			if ctagType == TAG_UUID {
				str = rdser.GetUuid()
			} else {
				str = rdser.GetVString()
			}
			switch {
			case k == reflect.String:
				v.SetString(str)
			case k == reflect.Slice, k == reflect.Array:
				elemK := v.Type().Elem().Kind()
				if elemK == reflect.String {
					if k == reflect.Slice {
						el := reflect.New(v.Type().Elem()).Elem()
						el.SetString(str)
						extSlice := reflect.Append(v, el)
						v.Set(extSlice)
					} else {
						panic(fmt.Errorf("can not put single value into the fixed size array of strings '%s'", str))
					}
				} else if elemK == reflect.Interface {
					if k == reflect.Slice {
						el := reflect.New(v.Type().Elem()).Elem()
						el.Set(reflect.ValueOf(str))
						extSlice := reflect.Append(v, el)
						v.Set(extSlice)
					} else {
						panic(fmt.Errorf("can not put single value into the fixed size array of interfaces '%s'", str))
					}
				} else {
					b, e := base64.StdEncoding.DecodeString(str)
					if e != nil {
						panic(fmt.Errorf("can not decode base64 '%s': %v", str, e))
					}
					v.SetBytes(b)
				}
			case k == reflect.Interface:
				v.Set(reflect.ValueOf(str))
			case k == reflect.Struct && v.Type().String() == "time.Time":
				tm, _ := time.Parse(time.RFC3339Nano, str)
				v.Set(reflect.ValueOf(tm))
			default:
				panic(fmt.Errorf("can not convert 'string' to '%s'", v.Type().Kind().String()))
			}
		default:
			if k == reflect.Slice {
				el := reflect.New(v.Type().Elem()).Elem()
				extSlice := reflect.Append(v, el)
				v.Set(extSlice)
				v = v.Index(v.Len() - 1)
				k = v.Type().Kind()
			} else if k == reflect.Array {
				panic(fmt.Errorf("can not put single value into the fixed size array"))
			}
			switch k {
			case reflect.Float32, reflect.Float64:
				v.SetFloat(asFloat(rdser, ctagType))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int64, reflect.Int32:
				v.SetInt(asInt(rdser, ctagType))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint64, reflect.Uint32:
				v.SetUint(uint64(asInt(rdser, ctagType)))
			case reflect.Interface:
				v.Set(reflect.ValueOf(asIface(rdser, ctagType)))
			case reflect.Bool:
				v.SetBool(asInt(rdser, ctagType) != 0)
			default:
				panic(fmt.Errorf("can not convert '%s' to '%s'", ctagTypeName(ctagType), v.Type().Kind().String()))
			}
		}
	}

	if isMap {
		if mv.Type().Elem().Kind() == reflect.Ptr {
			v = v.Addr()
		}
		name := dec.state.tagsMatcher.tag2name(ctagName)
		switch mv.Type().Key().Kind() {
		case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			nameint, _ := strconv.Atoi(name)
			mv.SetMapIndex(reflect.ValueOf(nameint).Convert(mv.Type().Key()), v)
		case reflect.Uint64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			nameuint, _ := strconv.Atoi(name)
			mv.SetMapIndex(reflect.ValueOf(nameuint).Convert(mv.Type().Key()), v)
		case reflect.String:
			mv.SetMapIndex(reflect.ValueOf(name), v)
		default:
			panic(fmt.Errorf("unsupported map key type '%s'", mv.Type().Key().Kind().String()))
		}
	}

	return true
}

func (dec *Decoder) DecodeCPtr(cptr uintptr, dest interface{}) (err error) {

	pl := &payloadIface{p: cptr, t: &dec.state.payloadType}

	dec.state.lock.RLock()
	defer dec.state.lock.RUnlock()

	tuple := pl.getBytes(0, 0)
	ser := &Serializer{buf: tuple}

	defer func() {
		if ret := recover(); ret != nil {
			if dec.loggerOwner != nil {
				if logger := dec.loggerOwner.GetLogger(); logger != nil {
					logger.Printf(bindings.ERROR,
						"Interface: %#v\nRead position: %d\nTags(v%d): %v\nPayload Type: %+v\nPayload Value: %v\nTags cache: %+v\nData dump:\n%s\nError: %v\nTagsMatcher: { state_token: 0x%08X, version: %d }\n",
						dest,
						ser.Pos(),
						dec.state.Version,
						dec.state.tagsMatcher.Names,
						dec.state.payloadType.Fields,
						pl.getAsMap(),
						dec.state.structCache,
						hex.Dump(ser.Bytes()),
						ret,
						uint32(dec.state.StateToken),
						dec.state.Version,
					)
				}
			}
			switch v := ret.(type) {
			case error:
				err = v
			default:
				err = fmt.Errorf("rq: DecodeCPtr error: %v", ret)
			}
		}
	}()

	fieldsoutcnt := make([]int, MaxIndexes)
	ctagsPath := make([]int16, 0, 16)

	dec.decodeValue(pl, ser, reflect.ValueOf(dest), fieldsoutcnt, ctagsPath)
	if !ser.Eof() {
		panic(fmt.Errorf("internal error - left unparsed data"))
	}
	return err
}

func (dec *Decoder) Finalize() {
	if dec.ser != nil {
		dec.ser.Close()
		dec.ser = nil
	}
}

func (dec *Decoder) Decode(cjson []byte, dest interface{}) (err error) {

	dec.state.lock.RLock()
	defer dec.state.lock.RUnlock()

	ser := &Serializer{buf: cjson}

	defer func() {
		if ret := recover(); ret != nil {
			if dec.loggerOwner != nil {
				if logger := dec.loggerOwner.GetLogger(); logger != nil {
					logger.Printf(bindings.ERROR,
						"Interface: %#v\nRead position: %d\nTags(v%d): %v\nTags cache: %+v\nData dump:\n%s\nError: %v\nTagsMatcher: { state_token: 0x%08X, version: %d }\n",
						dest,
						ser.Pos(),
						dec.state.Version,
						dec.state.tagsMatcher.Names,
						dec.state.structCache,
						hex.Dump(ser.Bytes()),
						ret,
						uint32(dec.state.StateToken),
						dec.state.Version,
					)
				}
			}
			switch v := ret.(type) {
			case error:
				err = v
			default:
				err = fmt.Errorf("rq: Decode error: %v", ret)
			}
		}
	}()

	fieldsoutcnt := make([]int, MaxIndexes)
	ctagsPath := make([]int16, 0, 16)

	dec.decodeValue(nil, ser, reflect.ValueOf(dest), fieldsoutcnt, ctagsPath)
	// if !ser.Eof() {
	// 	panic(fmt.Errorf("Internal error - left unparsed data"))
	// }
	return err
}
