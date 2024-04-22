#pragma once

#include "core/ft/limits.h"

namespace reindexer {

struct WordIdTypeBit {
	uint32_t step_num : 6;	// index in the array of the index build steps (IDataHolder::steps)
	uint32_t id : 26;		// index in the array of the unique words (DataHolder::words_)
};

static_assert(WordIdTypeBit{.step_num = kWordIdMaxStepVal, .id = 0}.step_num == kWordIdMaxStepVal, "Bitfield overflow");
static_assert(WordIdTypeBit{.step_num = 0, .id = kWordIdMaxIdVal}.id == kWordIdMaxIdVal, "Bitfield overflow");
static_assert(WordIdTypeBit{.step_num = 0, .id = kWordIdEmptyIdVal}.id == kWordIdEmptyIdVal, "Bitfield overflow");

union WordIdType {
	WordIdTypeBit b;
	uint32_t data = 0;

	bool IsEmpty() const noexcept { return b.id == kWordIdEmptyIdVal; }
	void SetEmpty() noexcept { b.id = kWordIdEmptyIdVal; }
	int32_t GetID() const noexcept { return b.id; }
	void SetID(int32_t v) noexcept {
		assertrx_dbg(v >= 0);
		assertrx_dbg(uint32_t(v) <= kWordIdMaxIdVal);
		b.id = v;
	}
};

struct WordIdTypeHash {
	std::size_t operator()(const WordIdType& k) const noexcept { return std::hash<uint32_t>()(k.data); }
};

struct WordIdTypeEqual {
	bool operator()(const WordIdType& lhs, const WordIdType& rhs) const noexcept { return lhs.data == rhs.data; }
};

struct WordIdTypeLess {
	bool operator()(const WordIdType& lhs, const WordIdType& rhs) const noexcept { return lhs.data < rhs.data; }
};

enum class FtUseExternStatuses : bool { Yes, No };

}  // namespace reindexer
