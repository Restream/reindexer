package cjson

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type Encoder struct {
	state       *State
	tagsMatcher *tagsMatcher
	tmUpdated   bool
}

type fieldInfo struct {
	ctagName    int
	kind        reflect.Kind
	elemKind    reflect.Kind
	isAnon      bool
	isNullable  bool
	isPrivate   bool
	isOmitEmpty bool
	isTime      bool
	isPtr       bool
}

func mkFieldInfo(v reflect.Value, ctagName int, anon bool) fieldInfo {
	t := v.Type()
	k := t.Kind()
	kk := k
	if k == reflect.Ptr {
		t = t.Elem()
		kk = t.Kind()
	}

	f := fieldInfo{
		isAnon:     anon,
		isNullable: (k == reflect.Ptr || k == reflect.Map || k == reflect.Slice || k == reflect.Interface),
		isPtr:      k == reflect.Ptr,
		kind:       kk,
		ctagName:   ctagName,
		isTime:     kk == reflect.Struct && t.String() == "time.Time",
	}
	if kk == reflect.Slice || kk == reflect.Array {
		f.elemKind = t.Elem().Kind()
	}

	return f
}

// non allocating version of strings.Split
func splitStr(in string, sep byte) (s1, s2, s3 string) {

	out := []*string{&s1, &s2, &s3}

	for i := 0; i < len(out); i++ {
		pos := strings.IndexByte(in, sep)

		if pos != -1 {
			*out[i] = in[:pos]
			in = in[pos+1:]
		} else {
			*out[i] = in
			break
		}
	}
	return
}

func parseStructField(sf reflect.StructField) (name string, skip, omitEmpty bool) {
	name, opt, _ := splitStr(sf.Tag.Get("json"), ',')
	if name == "-" || len(sf.PkgPath) != 0 {
		skip = true
	} else if name == "" {
		name = sf.Name
	}

	omitEmpty = (opt == "omitempty")
	return
}

func (enc *Encoder) encodeStruct(v reflect.Value, rdser *Serializer, idx []int) {
	for field := 0; field < v.NumField(); field++ {

		iidx := idx
		var ce *ctagsWCacheEntry
		if iidx == nil {
			// if idx is null - we have top level interface field,
			// in this case we can have any untyped data in deep, so
			// ctagsCache 'name path' -> type will not work
			// So here is using slow path: use reflect to obtain info about each field
			ce = &ctagsWCacheEntry{}
		} else {
			// We have not interface fields on top level, so using cache
			iidx = append(idx, field)
			ce = enc.state.ctagsWCache.Lookup(iidx, true)
		}

		if ce.ctagName == 0 && !ce.isPrivate {
			// No data in cache: use reflect to get data about field
			vv := v.Field(field)
			f := v.Type().Field(field)
			name, skip, omitempty := parseStructField(f)
			ctagName := 0
			if !skip {
				ctagName = enc.name2tag(name)
			}
			if enc.tmUpdated {
				// if tagsMatcher or lock is updated - we have temporary tags, do not cache them
				ce = &ctagsWCacheEntry{}
			}

			ce.fieldInfo = mkFieldInfo(vv, ctagName, f.Anonymous)
			ce.isPrivate = len(f.PkgPath) != 0 || skip
			ce.isOmitEmpty = omitempty
		}

		if !ce.isPrivate {
			// process field, except private unexported fields
			enc.encodeValue(v.Field(field), rdser, ce.fieldInfo, iidx)
		}
	}
}

func (enc *Encoder) name2tag(name string) int {

	tagName := enc.tagsMatcher.name2tag(name, false)

	if tagName == 0 {
		if !enc.tmUpdated {
			enc.tagsMatcher = &tagsMatcher{Tags: enc.state.tagsMatcher.Tags, Names: make(map[string]int)}
			for k, v := range enc.state.tagsMatcher.Names {
				enc.tagsMatcher.Names[k] = v
			}
			enc.tmUpdated = true
		}
		tagName = enc.tagsMatcher.name2tag(name, true)
	}
	return tagName
}

func (enc *Encoder) encodeMap(v reflect.Value, rdser *Serializer, idx []int) {
	keys := v.MapKeys()

	f := fieldInfo{}
	for i, k := range keys {
		keyName := ""
		vv := v.MapIndex(k)
		if i == 0 {
			f = mkFieldInfo(vv, 0, false)
		}

		switch k.Type().Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			keyName = strconv.FormatInt(k.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			keyName = strconv.FormatUint(k.Uint(), 10)
		case reflect.String:
			keyName = k.String()
		case reflect.Float32, reflect.Float64:
			keyName = strconv.FormatFloat(k.Float(), 'g', -1, 64)
		default:
			panic(fmt.Errorf("Unsupported map key type %s ", k.Type().Kind().String()))
		}

		f.ctagName = enc.name2tag(keyName)
		enc.encodeValue(vv, rdser, f, idx)
	}
}

func (enc *Encoder) encodeSlice(v reflect.Value, rdser *Serializer, f fieldInfo, idx []int) {
	l := v.Len()
	if l == 0 && f.isOmitEmpty {
		return
	}
	if f.elemKind == reflect.Uint8 {
		rdser.PutVarUInt(mkctag(TAG_STRING, f.ctagName, 0))
		rdser.PutVString(base64.StdEncoding.EncodeToString(v.Bytes()))
	} else {
		rdser.PutVarUInt(mkctag(TAG_ARRAY, f.ctagName, 0))

		subTag := TAG_OBJECT
		switch f.elemKind {
		case reflect.Int, reflect.Int16, reflect.Int64, reflect.Int8, reflect.Int32,
			reflect.Uint, reflect.Uint16, reflect.Uint64, reflect.Uint32:
			subTag = TAG_VARINT
		case reflect.Float32, reflect.Float64:
			subTag = TAG_DOUBLE
		case reflect.String:
			subTag = TAG_STRING
		case reflect.Bool:
			subTag = TAG_BOOL
		}

		rdser.PutUInt32(mkcarraytag(l, subTag))

		if f.kind == reflect.Slice {
			ptr := unsafe.Pointer(v.Pointer())

			switch f.elemKind {
			case reflect.Int:
				sl := (*[1 << 28]int)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Uint:
				sl := (*[1 << 28]uint)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Int32:
				sl := (*[1 << 28]int32)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Uint32:
				sl := (*[1 << 28]uint32)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Int16:
				sl := (*[1 << 29]int16)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Uint16:
				sl := (*[1 << 29]uint16)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Int64:
				sl := (*[1 << 27]int64)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Uint64:
				sl := (*[1 << 27]uint64)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Int8:
				sl := (*[1 << 30]int8)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVarInt(int64(v))
				}
			case reflect.Float32:
				sl := (*[1 << 28]float32)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutDouble(float64(v))
				}
			case reflect.Float64:
				sl := (*[1 << 27]float64)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutDouble(v)
				}
			case reflect.String:
				sl := (*[1 << 27]string)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutVString(v)
				}
			case reflect.Bool:
				sl := (*[1 << 30]bool)(ptr)[:l:l]
				for _, v := range sl {
					var vv uint64
					if v {
						vv = 1
					}
					rdser.PutVarUInt(vv)
				}
			default:
				if subTag != TAG_OBJECT {
					panic(fmt.Errorf("Internal error can't serialize array of type %s", f.elemKind.String()))
				}
				for i := 0; i < l; i++ {
					vv := v.Index(i)
					if i == 0 {
						f = mkFieldInfo(vv, 0, false)
						f.isOmitEmpty = false
					}
					enc.encodeValue(vv, rdser, f, idx)
				}
			}
		} else {
			switch f.elemKind {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				for i := 0; i < l; i++ {
					rdser.PutVarInt(v.Index(i).Int())
				}
			case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				for i := 0; i < l; i++ {
					rdser.PutVarUInt(v.Index(i).Uint())
				}
			case reflect.Float32, reflect.Float64:
				for i := 0; i < l; i++ {
					rdser.PutDouble(v.Index(i).Float())
				}
			case reflect.String:
				for i := 0; i < l; i++ {
					rdser.PutVString(v.Index(i).String())
				}
			case reflect.Bool:
				for i := 0; i < l; i++ {
					var vv uint64
					if v.Index(i).Bool() {
						vv = 1
					}
					rdser.PutVarUInt(vv)
				}
			default:
				if subTag != TAG_OBJECT {
					panic(fmt.Errorf("Internal error can't serialize array of type %s", f.elemKind.String()))
				}
				for i := 0; i < l; i++ {
					vv := v.Index(i)
					if i == 0 {
						f = mkFieldInfo(vv, 0, false)
						f.isOmitEmpty = false
					}
					enc.encodeValue(vv, rdser, f, idx)
				}
			}
		}
	}
}

func (enc *Encoder) encodeValue(v reflect.Value, rdser *Serializer, f fieldInfo, idx []int) {

	if f.isNullable && v.IsNil() {
		if !f.isOmitEmpty {
			rdser.PutVarUInt(mkctag(TAG_NULL, f.ctagName, 0))
		}
		return
	}

	if f.isPtr {
		v = v.Elem()
	}
	switch f.kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val := v.Int()
		if val != 0 || !f.isOmitEmpty {
			rdser.PutVarUInt(mkctag(TAG_VARINT, f.ctagName, 0))
			rdser.PutVarInt(val)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		val := v.Uint()
		if val != 0 || !f.isOmitEmpty {
			rdser.PutVarUInt(mkctag(TAG_VARINT, f.ctagName, 0))
			rdser.PutVarInt(int64(val))
		}
	case reflect.Float32, reflect.Float64:
		val := v.Float()
		if val != 0 || !f.isOmitEmpty {
			rdser.PutVarUInt(mkctag(TAG_DOUBLE, f.ctagName, 0))
			rdser.PutDouble(val)
		}
	case reflect.Bool:
		vv := 0
		if v.Bool() {
			vv = 1
		}
		if vv != 0 || !f.isOmitEmpty {
			rdser.PutVarUInt(mkctag(TAG_BOOL, f.ctagName, 0))
			rdser.PutVarUInt(uint64(vv))
		}
	case reflect.String:
		val := v.String()
		if len(val) != 0 || !f.isOmitEmpty {
			rdser.PutVarUInt(mkctag(TAG_STRING, f.ctagName, 0))
			rdser.PutVString(val)
		}
	case reflect.Slice, reflect.Array:
		enc.encodeSlice(v, rdser, f, idx)
	case reflect.Struct:
		if f.isTime && v.IsValid() {
			if tm, ok := v.Interface().(time.Time); ok {
				rdser.PutVarUInt(mkctag(TAG_STRING, f.ctagName, 0))
				rdser.PutVString(tm.Format(time.RFC3339Nano))
				return
			}
		}
		if !f.isAnon {
			rdser.PutVarUInt(mkctag(TAG_OBJECT, f.ctagName, 0))
			enc.encodeStruct(v, rdser, idx)
			rdser.PutVarUInt(mkctag(TAG_END, 0, 0))
		} else {
			enc.encodeStruct(v, rdser, idx)
		}
	case reflect.Map:
		rdser.PutVarUInt(mkctag(TAG_OBJECT, f.ctagName, 0))
		enc.encodeMap(v, rdser, idx)
		rdser.PutVarUInt(mkctag(TAG_END, 0, 0))
	case reflect.Interface:
		vv := v.Elem()
		enc.encodeValue(vv, rdser, mkFieldInfo(vv, f.ctagName, f.isAnon), nil)
	default:
		panic(fmt.Errorf("Unsupported type %s", f.kind.String()))
	}
}

func (enc *Encoder) Encode(src interface{}, wrser *Serializer) (stateToken int, err error) {

	v := reflect.ValueOf(src)
	enc.state.lock.Lock()

	pos := len(wrser.Bytes())
	wrser.PutVarUInt(TAG_END)
	wrser.PutUInt32(0)
	enc.tagsMatcher = &enc.state.tagsMatcher
	enc.tmUpdated = false
	enc.encodeValue(v, wrser, mkFieldInfo(v, 0, false), make([]int, 0, 10))

	if enc.tmUpdated {
		*(*uint32)(unsafe.Pointer(&wrser.Bytes()[pos+1])) = uint32(len(wrser.buf) - pos)
		enc.tagsMatcher.WriteUpdated(wrser)
	} else {
		wrser.TruncateStart(int(unsafe.Sizeof(uint32(0))) + 1)
	}
	stateToken = int(enc.state.StateToken)

	enc.state.lock.Unlock()
	return
}

func (enc *Encoder) EncodeRaw(src interface{}, wrser *Serializer) error {

	v := reflect.ValueOf(src)
	enc.state.lock.Lock()
	enc.tmUpdated = false

	enc.tagsMatcher = &enc.state.tagsMatcher
	enc.encodeValue(v, wrser, mkFieldInfo(v, 0, false), make([]int, 0, 10))
	if enc.tmUpdated {
		enc.state.tagsMatcher = *enc.tagsMatcher
	}

	enc.state.lock.Unlock()

	return nil

}
