#pragma once

#include "core/type_consts.h"

// ctag is varuint in the following format: RRRRRRNNNNNNNNNNNNNNTTT
// TTT - 3bit  type: one of TAG_XXXX
// NNN - 12bit index+1 of field name in tagsMatcher (0 if no name)
// RRR - 6bit index+1 of field in reindexer Payload (0 if no field)

class ctag {
public:
	ctag(int tag) : tag_(tag) {}
	ctag(int tagType, int tagName, int tagField = -1) : tag_(tagType | (tagName << typeBits) | ((tagField + 1) << (typeBits + nameBits))) {}
	explicit operator int() const { return tag_; }

	int Type() const { return tag_ & ((1 << typeBits) - 1); }
	int Name() const { return (tag_ >> typeBits) & ((1 << nameBits) - 1); }
	int Field() const { return (tag_ >> (typeBits + nameBits)) - 1; }

	static const int nameBits = 12;
	static const int typeBits = 3;

	const char *TypeName() {
		switch (Type()) {
			case TAG_VARINT:
				return "<varint>";
			case TAG_OBJECT:
				return "<object>";
			case TAG_END:
				return "<end>";
			case TAG_ARRAY:
				return "<array>";
			case TAG_BOOL:
				return "<bool>";
			case TAG_STRING:
				return "<string>";
			case TAG_DOUBLE:
				return "<double>";
			case TAG_NULL:
				return "<null>";
			default:
				return "<unknown>";
		}
	}

protected:
	int tag_;
};

// ctagarray is int32_t in the following format
// TTTTTTTTNNNNNNNNNNNNNNNNNNNNNNNN
// TTT - 3 bit type: one of TAG_VARINT,TAG_BOOL,TAG_STRING,TAG_DOUBLE,TAG_OBJECT. tag of array elements
// NNN - 24 bit: count of elements in array
class carraytag {
public:
	carraytag(int atag) : atag_(atag) {}
	carraytag(int count, int tag) : atag_(count | (tag << countBits)) {}
	int Tag() const { return atag_ >> countBits; }
	int Count() const { return atag_ & ((1 << countBits) - 1); }
	explicit operator int() const { return atag_; }

	static const int countBits = 24;

protected:
	int atag_;
};
