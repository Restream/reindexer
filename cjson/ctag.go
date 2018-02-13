package cjson

import "github.com/restream/reindexer/bindings"
import "fmt"

type ctag int

const typeBits = 3
const nameBits = 12

const (
	TAG_VARINT = bindings.TAG_VARINT
	TAG_OBJECT = bindings.TAG_OBJECT
	TAG_END    = bindings.TAG_END
	TAG_ARRAY  = bindings.TAG_ARRAY
	TAG_BOOL   = bindings.TAG_BOOL
	TAG_STRING = bindings.TAG_STRING
	TAG_DOUBLE = bindings.TAG_DOUBLE
	TAG_NULL   = bindings.TAG_NULL
)

func (c ctag) Name() int {
	return int(c>>typeBits) & ((1 << nameBits) - 1)
}

func (c ctag) Type() int {
	return int(c) & ((1 << typeBits) - 1)
}

func (c ctag) Field() int {
	return (int(c) >> (typeBits + nameBits)) - 1
}

func (c ctag) Dump() string {
	return fmt.Sprintf("(%s n:%d f:%d)", tagTypeName(c.Type()), c.Name(), c.Field())
}

func tagTypeName(tagType int) string {
	switch tagType {
	case TAG_VARINT:
		return "<varint>"
	case TAG_OBJECT:
		return "<object>"
	case TAG_END:
		return "<end>"
	case TAG_ARRAY:
		return "<array>"
	case TAG_BOOL:
		return "<bool>"
	case TAG_STRING:
		return "<string>"
	case TAG_DOUBLE:
		return "<double>"
	case TAG_NULL:
		return "<null>"
	default:
		return fmt.Sprintf("<unknown %d>", tagType)
	}
}

func mkctag(ctagType int, ctagName int, ctagField int) uint64 {
	return uint64(ctagType | (ctagName << typeBits) | (ctagField << (nameBits + typeBits)))
}

const countBits = 24

type carraytag uint32

func (t carraytag) Count() int {
	return int(t) & ((1 << countBits) - 1)
}

func (t carraytag) Tag() int {
	return int(t) >> countBits
}

func mkcarraytag(count int, tag int) uint32 {
	return uint32(count | (tag << countBits))
}
