#pragma once

#include "estl/h_vector.h"

namespace reindexer {

static constexpr int maxIndexes = 64;

class FieldsSet : public h_vector<uint8_t, maxIndexes> {
public:
	void push_back(int f) {
		if (!(mask_ & (1ULL << f))) {
			mask_ |= 1ULL << f;
			h_vector<uint8_t, maxIndexes>::push_back(f);
		}
	}
	bool contains(int f) const { return mask_ & (1ULL << f); }
	bool contains(const FieldsSet &f) const { return (mask_ & f.mask_) == mask_; }
	void clear() {
		h_vector<uint8_t, maxIndexes>::clear();
		mask_ = 0;
	}
	bool containsAll(int f) const { return (((1 << f) - 1) & mask_) == (1ULL << f) - 1ULL; }
	bool operator==(const FieldsSet &f) const { return mask_ == f.mask_; }
	bool operator!=(const FieldsSet &f) const { return mask_ != f.mask_; }

protected:
	uint64_t mask_ = 0;
};

}  // namespace reindexer
