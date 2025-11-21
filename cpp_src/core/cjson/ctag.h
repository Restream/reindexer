#pragma once

#include "core/tag_name_index.h"
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

class [[nodiscard]] ctag {
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

	RX_ALWAYS_INLINE constexpr explicit ctag(TagType tagType) noexcept : ctag{tagType, TagName::Empty()} {}
	RX_ALWAYS_INLINE constexpr ctag(TagType tagType, TagName tagName, int tagField = -1) noexcept
		: tag_{(uint32_t(tagType) & kTypeMask) | (uint32_t(tagName.AsNumber()) << kTypeBits) | (uint32_t(tagField + 1) << kFieldOffset) |
			   ((uint32_t(tagType) & kInvertedTypeMask) << kType1Offset)} {
		assertrx(tagName.AsNumber() >= 0);
		assertrx(tagName.AsNumber() <= kNameMax);
		assertrx(tagField >= -1);
		assertrx(tagField + 1 < (1 << kFieldBits));
	}

	RX_ALWAYS_INLINE constexpr TagType Type() const noexcept { return typeImpl(tag_); }
	RX_ALWAYS_INLINE constexpr TagName Name() const noexcept { return nameImpl(tag_); }
	RX_ALWAYS_INLINE constexpr int Field() const noexcept { return fieldImpl(tag_); }

	RX_ALWAYS_INLINE constexpr bool operator==(ctag other) const noexcept { return tag_ == other.tag_; }
	RX_ALWAYS_INLINE constexpr bool operator!=(ctag other) const noexcept { return !operator==(other); }

private:
	RX_ALWAYS_INLINE explicit constexpr ctag(uint32_t tag) noexcept : ctag{typeImpl(tag), nameImpl(tag), fieldImpl(tag)} {
		assertrx_dbg(tag == tag_);
	}
	RX_ALWAYS_INLINE explicit constexpr ctag(uint64_t tag) noexcept : ctag{typeImpl(tag), nameImpl(tag), fieldImpl(tag)} {
		assertrx_dbg(tag == tag_);
	}
	RX_ALWAYS_INLINE constexpr static TagType typeImpl(uint32_t tag) noexcept {
		return static_cast<TagType>((tag & kTypeMask) | ((tag >> kType1Offset) & kInvertedTypeMask));
	}
	RX_ALWAYS_INLINE constexpr static TagName nameImpl(uint32_t tag) noexcept { return TagName((tag >> kTypeBits) & kNameMask); }
	RX_ALWAYS_INLINE constexpr static int fieldImpl(uint32_t tag) noexcept { return int((tag >> kFieldOffset) & kFieldMask) - 1; }
	RX_ALWAYS_INLINE constexpr uint32_t asNumber() const noexcept { return tag_; }

	uint32_t tag_;
};

constexpr ctag kCTagEnd{TAG_END};

// ctagarray is uint32_t in the following format
// TTTTTTTTNNNNNNNNNNNNNNNNNNNNNNNN
// TTT - 6 bit type: one of TAG_VARINT,TAG_BOOL,TAG_STRING,TAG_DOUBLE,TAG_OBJECT. tag of array elements
// NNN - 24 bit: count of elements in array
class [[nodiscard]] carraytag {
	friend class reindexer::Serializer;
	friend class reindexer::WrSerializer;

	static constexpr unsigned kCountBits = 24;
	static constexpr unsigned kTypeBits = 6;
	static constexpr uint32_t kCountMask = (uint32_t(1) << kCountBits) - uint32_t(1);
	static constexpr uint32_t kTypeMask = (uint32_t(1) << kTypeBits) - uint32_t(1);

public:
	RX_ALWAYS_INLINE constexpr carraytag(uint32_t count, TagType tag) noexcept : atag_(count | (uint32_t(tag) << kCountBits)) {
		assertrx(count < (uint32_t(1) << kCountBits));
	}
	RX_ALWAYS_INLINE constexpr TagType Type() const noexcept { return typeImpl(atag_); }
	RX_ALWAYS_INLINE constexpr uint32_t Count() const noexcept { return countImpl(atag_); }

	RX_ALWAYS_INLINE constexpr bool operator==(carraytag other) const noexcept { return atag_ == other.atag_; }
	RX_ALWAYS_INLINE constexpr bool operator!=(carraytag other) const noexcept { return !operator==(other); }

private:
	RX_ALWAYS_INLINE explicit constexpr carraytag(uint32_t atag) noexcept : carraytag{countImpl(atag), typeImpl(atag)} {
		assertrx_dbg(atag == atag_);
	}
	RX_ALWAYS_INLINE constexpr uint32_t asNumber() const noexcept { return atag_; }
	RX_ALWAYS_INLINE static constexpr TagType typeImpl(uint32_t atag) noexcept {
		assertrx_dbg(((atag >> kCountBits) & kTypeMask) <= kMaxTagType);
		return static_cast<TagType>((atag >> kCountBits) & kTypeMask);	// NOLINT(*EnumCastOutOfRange)
	}
	RX_ALWAYS_INLINE static constexpr uint32_t countImpl(uint32_t atag) noexcept { return atag & kCountMask; }

	uint32_t atag_;
};

}  // namespace reindexer
