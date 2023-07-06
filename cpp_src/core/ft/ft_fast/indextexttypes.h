#pragma once

namespace reindexer {

const uint32_t kWordIdMaxIdVal = 0x7FFFFFF;
const uint32_t kWordIdMaxStepVal = 0x7F;

struct WordIdTypeBit {
	uint32_t step_num : 4;	// index in array IDataHolder::steps
	uint32_t id : 27;		// index in array DataHolder::words_
	uint32_t multi_flag : 1;
};

union WordIdType {
	WordIdType() noexcept { b.multi_flag = 0; }
	WordIdTypeBit b;
	uint32_t data;

	bool isEmpty() const noexcept { return b.id == kWordIdMaxIdVal; }
	void setEmpty() noexcept { b.id = kWordIdMaxIdVal; }

	operator uint32_t() const noexcept { return data; }
	WordIdType operator=(const uint32_t& rhs) {
		data = rhs;
		return *this;
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
