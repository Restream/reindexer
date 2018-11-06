#pragma once

namespace reindexer {

const uint32_t kWordIdMaxIdVal = 0x7FFFFFF;
const uint32_t kWordIdMaxStepVal = 0x7F;

struct WordIdTypeBit {
	uint32_t step_num : 4;
	uint32_t id : 27;
	uint32_t multi_flag : 1;
};

union WordIdType {
	WordIdType() { b.multi_flag = 0; }
	WordIdTypeBit b;
	uint32_t data;

	bool isEmpty() const { return b.id == kWordIdMaxIdVal; }
	void setEmpty() { b.id = kWordIdMaxIdVal; }

	operator uint32_t() const { return data; }
	WordIdType operator=(const uint32_t& rhs) {
		data = rhs;
		return *this;
	}
};

struct WordIdTypeHash {
	std::size_t operator()(const WordIdType& k) const { return std::hash<uint32_t>()(k.data); }
};

struct WordIdTypequal {
	bool operator()(const WordIdType& lhs, const WordIdType& rhs) const { return lhs.data == rhs.data; }
};
}  // namespace reindexer
