package cjson

import "fmt"

type ctag uint32

const (
	typeBits     = 3
	typeMask     = (1 << typeBits) - 1
	nameBits     = 12
	nameMask     = (1 << nameBits) - 1
	fieldBits    = 10
	fieldMask    = (1 << fieldBits) - 1
	reservedBits = 4
	type2Offset  = typeBits + nameBits + fieldBits + reservedBits
)

const (
	TAG_VARINT = 0
	TAG_DOUBLE = 1
	TAG_STRING = 2
	TAG_BOOL   = 3
	TAG_NULL   = 4
	TAG_ARRAY  = 5
	TAG_OBJECT = 6
	TAG_END    = 7
	TAG_UUID   = 8
	TAG_FLOAT  = 9
)

func (c ctag) Name() int16 {
	return int16(c>>typeBits) & nameMask
}

func (c ctag) Type() int16 {
	return int16(int(c)&typeMask) | int16(((int(c)>>type2Offset)&typeMask)<<typeBits)
}

func (c ctag) Field() int16 {
	return int16((int(c)>>(typeBits+nameBits))&fieldMask) - 1
}

func (c ctag) Dump() string {
	return fmt.Sprintf("(%s n:%d f:%d)", tagTypeName(c.Type()), c.Name(), c.Field())
}

func tagTypeName(tagType int16) string {
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
	case TAG_UUID:
		return "<uuid>"
	case TAG_FLOAT:
		return "<float>"
	default:
		return fmt.Sprintf("<unknown %d>", tagType)
	}
}

func mkctag(ctagType int, ctagName int, ctagField int) ctag {
	return ctag((ctagType & typeMask) | (ctagName << typeBits) | (ctagField << (nameBits + typeBits)) | (((ctagType >> typeBits) & typeMask) << type2Offset))
}

const (
	countBits = 24
	countMask = (1 << countBits) - 1
)

type carraytag uint32

func (t carraytag) Count() int {
	return int(t) & countMask
}

func (t carraytag) Tag() int16 {
	return int16(int(t) >> countBits)
}

func mkcarraytag(count int, tag int) carraytag {
	return carraytag(count | (tag << countBits))
}
