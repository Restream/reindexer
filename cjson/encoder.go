package cjson

import (
	"bytes"
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
	isUuid      bool
}

func SplitFieldOptions(str string) []string {
	words := make([]string, 0)
	var word bytes.Buffer
	strLen := len(str)
	for i := 0; i < strLen; i++ {
		if str[i] == '\\' && i < strLen-1 && str[i+1] == ',' {
			word.WriteByte(str[i+1])
			i++
			continue
		}
		if str[i] == ',' {
			words = append(words, word.String())
			word.Reset()
			continue
		}
		word.WriteByte(str[i])
		if i == strLen-1 {
			words = append(words, word.String())
			word.Reset()
			continue
		}
	}
	return words
}

func isUuid(sf reflect.StructField) bool {
	tagsSlice := strings.SplitN(sf.Tag.Get("reindex"), ",", 3)
	if len(tagsSlice) < 3 || tagsSlice[1] == "ttl" {
		return false
	}
	for _, opt := range SplitFieldOptions(tagsSlice[2]) {
		if opt == "uuid" {
			return true
		}
	}
	return false
}

func mkFieldInfo(v reflect.Value, ctagName int, sf reflect.StructField) fieldInfo {
	t := v.Type()
	k := t.Kind()
	kk := k
	if k == reflect.Ptr {
		t = t.Elem()
		kk = t.Kind()
	}

	f := fieldInfo{
		isAnon:     sf.Anonymous,
		isNullable: (k == reflect.Ptr || k == reflect.Map || k == reflect.Slice || k == reflect.Interface),
		isPtr:      k == reflect.Ptr,
		kind:       kk,
		ctagName:   ctagName,
		isTime:     kk == reflect.Struct && t.String() == "time.Time",
	}
	if kk == reflect.Slice || kk == reflect.Array {
		f.elemKind = t.Elem().Kind()
	}
	if kk == reflect.String || ((kk == reflect.Slice || kk == reflect.Array) && f.elemKind == reflect.String) {
		f.isUuid = isUuid(sf)
	}

	return f
}

// non allocating version of strings.Split
func splitStr(in string, sep byte) (s1, s2 string) {
	if pos := strings.IndexByte(in, sep); pos != -1 {
		s1 = in[:pos]
		s2 = in[pos+1:]
	} else {
		s1 = in
	}
	return
}

func parseStructField(sf reflect.StructField) (name string, skip, omitEmpty bool) {
	name, opts := splitStr(sf.Tag.Get("json"), ',')
	if name == "-" || len(sf.PkgPath) != 0 {
		skip = true
	} else if name == "" {
		name = sf.Name
	}

	omitEmpty = strings.Contains(opts, "omitempty")

	if !skip {
		_, opts = splitStr(sf.Tag.Get("reindex"), ',')
		skip = strings.Contains(opts, "joined") || strings.Contains(opts, "composite")
	}

	return
}

func (enc *Encoder) encodeStruct(v reflect.Value, rdser *Serializer, idx []int) error {
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
			ce = enc.state.ctagsWCache.Lookup(iidx)
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

			ce.fieldInfo = mkFieldInfo(vv, ctagName, f)
			ce.isPrivate = len(f.PkgPath) != 0 || skip
			ce.isOmitEmpty = omitempty
		}

		if !ce.isPrivate {
			// process field, except private unexported fields
			err := enc.encodeValue(v.Field(field), rdser, ce.fieldInfo, iidx)
			if err != nil {
				return err
			}
		}
	}
	return nil
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

func (enc *Encoder) encodeMap(v reflect.Value, rdser *Serializer, idx []int) error {
	keys := v.MapKeys()

	f := fieldInfo{}
	for i, k := range keys {
		keyName := ""
		vv := v.MapIndex(k)
		if i == 0 {
			f = mkFieldInfo(vv, 0, reflect.StructField{})
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
		err := enc.encodeValue(vv, rdser, f, idx)
		if err != nil {
			return err
		}
	}
	return nil
}

var hexCharToUint = [256]uint64{
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255, 255, 255, 255, 255, 255,
	255, 10, 11, 12, 13, 14, 15, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 10, 11, 12, 13, 14, 15, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
}

func generateError(ch byte, str string) (res [2]uint64, err error) {
	if ch == '-' {
		err = fmt.Errorf("Invalid UUID format: '%s'", str)
	} else {
		err = fmt.Errorf("UUID cannot contain char '%c': '%s'", ch, str)
	}
	return
}

func ParseUuid(str string) (res [2]uint64, err error) {
	switch len(str) {
	case 0:
		return
	case 32:
		ch := str[0]
		num := hexCharToUint[ch]
		if num > 15 {
			return generateError(ch, str)
		}
		res[0] = num << 60
		ch = str[1]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 56
		ch = str[2]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 52
		ch = str[3]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 48
		ch = str[4]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 44
		ch = str[5]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 40
		ch = str[6]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 36
		ch = str[7]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 32
		ch = str[8]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 28
		ch = str[9]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 24
		ch = str[10]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 20
		ch = str[11]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 16
		ch = str[12]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 12
		ch = str[13]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 8
		ch = str[14]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 4
		ch = str[15]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num
		ch = str[16]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] = num << 60
		ch = str[17]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 56
		ch = str[18]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 52
		ch = str[19]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 48
		ch = str[20]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 44
		ch = str[21]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 40
		ch = str[22]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 36
		ch = str[23]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 32
		ch = str[24]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 28
		ch = str[25]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 24
		ch = str[26]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 20
		ch = str[27]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 16
		ch = str[28]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 12
		ch = str[29]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 8
		ch = str[30]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 4
		ch = str[31]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num
	case 36:
		if str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-' {
			err = fmt.Errorf("Invalid UUID format: '%s'", str)
			return
		}
		ch := str[0]
		num := hexCharToUint[ch]
		if num > 15 {
			return generateError(ch, str)
		}
		res[0] = num << 60
		ch = str[1]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 56
		ch = str[2]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 52
		ch = str[3]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 48
		ch = str[4]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 44
		ch = str[5]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 40
		ch = str[6]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 36
		ch = str[7]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 32
		ch = str[9]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 28
		ch = str[10]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 24
		ch = str[11]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 20
		ch = str[12]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 16
		ch = str[14]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 12
		ch = str[15]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 8
		ch = str[16]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num << 4
		ch = str[17]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[0] |= num
		ch = str[19]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] = num << 60
		ch = str[20]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 56
		ch = str[21]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 52
		ch = str[22]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 48
		ch = str[24]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 44
		ch = str[25]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 40
		ch = str[26]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 36
		ch = str[27]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 32
		ch = str[28]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 28
		ch = str[29]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 24
		ch = str[30]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 20
		ch = str[31]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 16
		ch = str[32]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 12
		ch = str[33]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 8
		ch = str[34]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num << 4
		ch = str[35]
		if num = hexCharToUint[ch]; num > 15 {
			return generateError(ch, str)
		}
		res[1] |= num
	default:
		err = fmt.Errorf("UUID should consist of 32 hexadecimal digits: '%s'", str)
		return
	}
	if (res[0] != 0 || res[1] != 0) && (res[1]>>63) == 0 {
		err = fmt.Errorf("Variant 0 of UUID is unsupported: '%s'", str)
	}
	return
}

func (enc *Encoder) encodeSlice(v reflect.Value, rdser *Serializer, f fieldInfo, idx []int) error {
	l := v.Len()
	if l == 0 && f.isOmitEmpty {
		return nil
	}
	if f.elemKind == reflect.Uint8 {
		rdser.PutCTag(mkctag(TAG_STRING, f.ctagName, 0))
		rdser.PutVString(base64.StdEncoding.EncodeToString(v.Bytes()))
	} else {
		rdser.PutCTag(mkctag(TAG_ARRAY, f.ctagName, 0))

		subTag := TAG_OBJECT
		switch f.elemKind {
		case reflect.Int, reflect.Int16, reflect.Int64, reflect.Int8, reflect.Int32,
			reflect.Uint, reflect.Uint16, reflect.Uint64, reflect.Uint32:
			subTag = TAG_VARINT
		case reflect.Float32:
			subTag = TAG_FLOAT
		case reflect.Float64:
			subTag = TAG_DOUBLE
		case reflect.String:
			if f.isUuid {
				subTag = TAG_UUID
			} else {
				subTag = TAG_STRING
			}
		case reflect.Bool:
			subTag = TAG_BOOL
		}

		rdser.PutCArrayTag(mkcarraytag(l, subTag))

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
					rdser.PutFloat32(v)
				}
			case reflect.Float64:
				sl := (*[1 << 27]float64)(ptr)[:l:l]
				for _, v := range sl {
					rdser.PutDouble(v)
				}
			case reflect.String:
				sl := (*[1 << 27]string)(ptr)[:l:l]
				if f.isUuid {
					for _, v := range sl {
						uuid, err := ParseUuid(v)
						if err != nil {
							return err
						}
						rdser.PutUuid(uuid)
					}
				} else {
					for _, v := range sl {
						rdser.PutVString(v)
					}
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
					return fmt.Errorf("Internal error can't serialize array of type %s", f.elemKind.String())
				}
				for i := 0; i < l; i++ {
					vv := v.Index(i)
					if i == 0 {
						f = mkFieldInfo(vv, 0, reflect.StructField{})
						f.isOmitEmpty = false
					}
					err := enc.encodeValue(vv, rdser, f, idx)
					if err != nil {
						return err
					}
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
			case reflect.Float32:
				for i := 0; i < l; i++ {
					rdser.PutFloat32(float32(v.Index(i).Float()))
				}
			case reflect.Float64:
				for i := 0; i < l; i++ {
					rdser.PutDouble(v.Index(i).Float())
				}
			case reflect.String:
				if f.isUuid {
					for i := 0; i < l; i++ {
						uuid, err := ParseUuid(v.Index(i).String())
						if err != nil {
							return err
						}
						rdser.PutUuid(uuid)
					}
				} else {
					for i := 0; i < l; i++ {
						rdser.PutVString(v.Index(i).String())
					}
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
					return fmt.Errorf("Internal error can't serialize array of type %s", f.elemKind.String())
				}
				for i := 0; i < l; i++ {
					vv := v.Index(i)
					if i == 0 {
						f = mkFieldInfo(vv, 0, reflect.StructField{})
						f.isOmitEmpty = false
					}
					err := enc.encodeValue(vv, rdser, f, idx)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (enc *Encoder) encodeValue(v reflect.Value, rdser *Serializer, f fieldInfo, idx []int) error {

	if f.isNullable && v.IsNil() {
		if !f.isOmitEmpty {
			rdser.PutCTag(mkctag(TAG_NULL, f.ctagName, 0))
		}
		return nil
	}

	if f.isPtr {
		v = v.Elem()
	}
	switch f.kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val := v.Int()
		if val != 0 || !f.isOmitEmpty {
			rdser.PutCTag(mkctag(TAG_VARINT, f.ctagName, 0))
			rdser.PutVarInt(val)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		val := v.Uint()
		if val != 0 || !f.isOmitEmpty {
			rdser.PutCTag(mkctag(TAG_VARINT, f.ctagName, 0))
			rdser.PutVarInt(int64(val))
		}
	case reflect.Float32:
		val := v.Float()
		if val != 0 || !f.isOmitEmpty {
			// rdser.PutCTag(mkctag(TAG_FLOAT, f.ctagName, 0))
			// rdser.PutFloat32(float32(val))
			// FIXME: Encoding float64 for the some kind of compatibility. We should encode float32 here after full migration to v5
			rdser.PutCTag(mkctag(TAG_DOUBLE, f.ctagName, 0))
			rdser.PutDouble(val)
		}
	case reflect.Float64:
		val := v.Float()
		if val != 0 || !f.isOmitEmpty {
			rdser.PutCTag(mkctag(TAG_DOUBLE, f.ctagName, 0))
			rdser.PutDouble(val)
		}
	case reflect.Bool:
		vv := 0
		if v.Bool() {
			vv = 1
		}
		if vv != 0 || !f.isOmitEmpty {
			rdser.PutCTag(mkctag(TAG_BOOL, f.ctagName, 0))
			rdser.PutVarUInt(uint64(vv))
		}
	case reflect.String:
		val := v.String()
		if f.isUuid {
			uuid, err := ParseUuid(val)
			if err != nil {
				return err
			}
			rdser.PutCTag(mkctag(TAG_UUID, f.ctagName, 0))
			rdser.PutUuid(uuid)
		} else if len(val) != 0 || !f.isOmitEmpty {
			rdser.PutCTag(mkctag(TAG_STRING, f.ctagName, 0))
			rdser.PutVString(val)
		}
	case reflect.Slice, reflect.Array:
		err := enc.encodeSlice(v, rdser, f, idx)
		if err != nil {
			return err
		}
	case reflect.Struct:
		if f.isTime && v.IsValid() {
			if tm, ok := v.Interface().(time.Time); ok {
				rdser.PutCTag(mkctag(TAG_STRING, f.ctagName, 0))
				rdser.PutVString(tm.Format(time.RFC3339Nano))
				return nil
			}
		}
		if !f.isAnon {
			rdser.PutCTag(mkctag(TAG_OBJECT, f.ctagName, 0))
			err := enc.encodeStruct(v, rdser, idx)
			if err != nil {
				return err
			}
			rdser.PutCTag(mkctag(TAG_END, 0, 0))
		} else {
			err := enc.encodeStruct(v, rdser, idx)
			if err != nil {
				return err
			}
		}
	case reflect.Map:
		rdser.PutCTag(mkctag(TAG_OBJECT, f.ctagName, 0))
		err := enc.encodeMap(v, rdser, idx)
		if err != nil {
			return err
		}
		rdser.PutCTag(mkctag(TAG_END, 0, 0))
	case reflect.Interface:
		vv := v.Elem()
		sf := reflect.StructField{
			Anonymous: f.isAnon,
		}
		err := enc.encodeValue(vv, rdser, mkFieldInfo(vv, f.ctagName, sf), nil)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unsupported type %s", f.kind.String())
	}
	return nil
}

func (enc *Encoder) Encode(src interface{}, wrser *Serializer) (stateToken int, err error) {

	v := reflect.ValueOf(src)
	enc.state.lock.Lock()
	defer enc.state.lock.Unlock()

	pos := len(wrser.Bytes())
	wrser.PutVarUInt(TAG_END)
	wrser.PutUInt32(0)
	enc.tagsMatcher = &enc.state.tagsMatcher
	enc.tmUpdated = false
	err = enc.encodeValue(v, wrser, mkFieldInfo(v, 0, reflect.StructField{}), make([]int, 0, 10))
	if err != nil {
		return
	}

	if enc.tmUpdated {
		*(*uint32)(unsafe.Pointer(&wrser.Bytes()[pos+1])) = uint32(len(wrser.buf) - pos)
		enc.tagsMatcher.WriteUpdated(wrser)
	} else {
		wrser.TruncateStart(int(unsafe.Sizeof(uint32(0))) + 1)
	}
	stateToken = int(enc.state.StateToken)
	return
}

func (enc *Encoder) EncodeRaw(src interface{}, wrser *Serializer) error {

	v := reflect.ValueOf(src)
	enc.state.lock.Lock()
	defer enc.state.lock.Unlock()
	enc.tmUpdated = false

	enc.tagsMatcher = &enc.state.tagsMatcher
	err := enc.encodeValue(v, wrser, mkFieldInfo(v, 0, reflect.StructField{}), make([]int, 0, 10))
	if err != nil {
		return err
	}
	if enc.tmUpdated {
		enc.state.tagsMatcher = *enc.tagsMatcher
	}
	return nil
}
