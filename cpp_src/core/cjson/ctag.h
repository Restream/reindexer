#pragma once

#include <limits>
#include "core/type_consts.h"
#include "tools/assertrx.h"

// ctag is varuint in the following format:
// |31     29|28        25|24     15|14     3|2     0|
// |  TYPE1  |  RESERVED  |  FIELD  |  NAME  | TYPE0 |
// TYPE0 and TYPE1 - 6bit  type: one of TAG_XXXX
// NAME - 12bit index+1 of field name in tagsMatcher (0 if no name)
// FIELD - 10bit index+1 of field in reindexer Payload (0 if no field)

namespace reindexer {
class Serializer;
class WrSerializer;
}  // namespace reindexer

class ctag {
	friend class reindexer::Serializer;
	friend class reindexer::WrSerializer;

	static constexpr unsigned kTypeBits = 3;
	static constexpr unsigned kNameBits = 12;
	static constexpr unsigned kFieldBits = 10;
	static constexpr unsigned kReservedBits = 4;
	static constexpr unsigned kFieldOffset = kTypeBits + kNameBits;
	static constexpr unsigned kType1Offset = kNameBits + kFieldBits + kReservedBits;
	static constexpr uint32_t kTypeMask = (uint32_t(1) << kTypeBits) - uint32_t(1);
	static constexpr uint32_t kInvertedTypeMask = ~kTypeMask;
	static constexpr uint32_t kFieldMask = (uint32_t(1) << kFieldBits) - uint32_t(1);

public:
	static constexpr uint32_t kNameMask = (uint32_t(1) << kNameBits) - uint32_t(1);
	static constexpr int kNameMax = (1 << kNameBits) - 1;

	constexpr explicit ctag(TagType tagType) noexcept : ctag{tagType, 0} {}
	constexpr ctag(TagType tagType, int tagName, int tagField = -1) noexcept
		: tag_{(uint32_t(tagType) & kTypeMask) | (uint32_t(tagName) << kTypeBits) | (uint32_t(tagField + 1) << kFieldOffset) |
			   ((uint32_t(tagType) & kInvertedTypeMask) << kType1Offset)} {
		assertrx(tagName >= 0);
		assertrx(tagName <= kNameMax);
		assertrx(tagField >= -1);
		assertrx(tagField + 1 < (1 << kFieldBits));
	}

	[[nodiscard]] constexpr TagType Type() const noexcept { return typeImpl(tag_); }
	[[nodiscard]] constexpr int Name() const noexcept { return nameImpl(tag_); }
	[[nodiscard]] constexpr int Field() const noexcept { return fieldImpl(tag_); }

	[[nodiscard]] constexpr bool operator==(ctag other) const noexcept { return tag_ == other.tag_; }
	[[nodiscard]] constexpr bool operator!=(ctag other) const noexcept { return !operator==(other); }

private:
	explicit constexpr ctag(uint32_t tag) noexcept : ctag{typeImpl(tag), nameImpl(tag), fieldImpl(tag)} { assertrx(tag == tag_); }
	explicit constexpr ctag(uint64_t tag) noexcept : ctag{typeImpl(tag), nameImpl(tag), fieldImpl(tag)} { assertrx(tag == tag_); }
	[[nodiscard]] constexpr static TagType typeImpl(uint32_t tag) noexcept {
		return static_cast<TagType>((tag & kTypeMask) | ((tag >> kType1Offset) & kInvertedTypeMask));
	}
	[[nodiscard]] constexpr static int nameImpl(uint32_t tag) noexcept { return (tag >> kTypeBits) & kNameMask; }
	[[nodiscard]] constexpr static int fieldImpl(uint32_t tag) noexcept { return int((tag >> kFieldOffset) & kFieldMask) - 1; }
	[[nodiscard]] constexpr uint32_t asNumber() const noexcept { return tag_; }

	uint32_t tag_;
};

constexpr ctag kCTagEnd{TAG_END};

// ctagarray is uint32_t in the following format
// TTTTTTTTNNNNNNNNNNNNNNNNNNNNNNNN
// TTT - 6 bit type: one of TAG_VARINT,TAG_BOOL,TAG_STRING,TAG_DOUBLE,TAG_OBJECT. tag of array elements
// NNN - 24 bit: count of elements in array
class carraytag {
	friend class reindexer::Serializer;
	friend class reindexer::WrSerializer;

	static constexpr unsigned kCountBits = 24;
	static constexpr unsigned kTypeBits = 6;
	static constexpr uint32_t kCountMask = (uint32_t(1) << kCountBits) - uint32_t(1);
	static constexpr uint32_t kTypeMask = (uint32_t(1) << kTypeBits) - uint32_t(1);

public:
	constexpr carraytag(uint32_t count, TagType tag) noexcept : atag_(count | (uint32_t(tag) << kCountBits)) {
		assertrx(count < (uint32_t(1) << kCountBits));
	}
	[[nodiscard]] constexpr TagType Type() const noexcept { return typeImpl(atag_); }
	[[nodiscard]] constexpr uint32_t Count() const noexcept { return countImpl(atag_); }

	[[nodiscard]] constexpr bool operator==(carraytag other) const noexcept { return atag_ == other.atag_; }
	[[nodiscard]] constexpr bool operator!=(carraytag other) const noexcept { return !operator==(other); }

private:
	explicit constexpr carraytag(uint32_t atag) noexcept : carraytag{countImpl(atag), typeImpl(atag)} { assertrx(atag == atag_); }
	[[nodiscard]] constexpr uint32_t asNumber() const noexcept { return atag_; }
	[[nodiscard]] static constexpr TagType typeImpl(uint32_t atag) noexcept {
		return static_cast<TagType>((atag >> kCountBits) & kTypeMask);
	}
	[[nodiscard]] static constexpr uint32_t countImpl(uint32_t atag) noexcept { return atag & kCountMask; }

	uint32_t atag_;
};
