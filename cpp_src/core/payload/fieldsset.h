#pragma once

#include <algorithm>
#include "estl/h_vector.h"

namespace reindexer {

static constexpr int maxIndexes = 64;

using base_fields_set = h_vector<uint8_t, 6>;

class FieldsSet : protected base_fields_set {
public:
	using base_fields_set::begin;
	using base_fields_set::end;
	using base_fields_set::iterator;
	using base_fields_set::size;
	using base_fields_set::empty;
	using base_fields_set::operator[];
	FieldsSet() = default;
	FieldsSet(std::initializer_list<int> l) : mask_(0) {
		for (auto f : l) push_back(f);
	}

	void push_back(int f) {
		assert(f < maxIndexes);
		if (!contains(f)) {
			mask_ |= 1ULL << f;
			base_fields_set::push_back(f);
		}
	}
	void erase(int f) {
		if (contains(f)) {
			auto it = std::find(begin(), end(), f);
			assert(it != end());
			base_fields_set::erase(it);
			mask_ &= ~(1ULL << f);
		}
	}

	bool contains(int f) const { return mask_ & (1ULL << f); }
	bool contains(const FieldsSet &f) const { return (mask_ & f.mask_) == mask_; }
	void clear() {
		base_fields_set::clear();
		mask_ = 0;
	}
	bool containsAll(int f) const { return (((1 << f) - 1) & mask_) == (1ULL << f) - 1ULL; }
	bool operator==(const FieldsSet &f) const { return mask_ == f.mask_; }
	bool operator!=(const FieldsSet &f) const { return mask_ != f.mask_; }

protected:
	uint64_t mask_ = 0;
};

}  // namespace reindexer
