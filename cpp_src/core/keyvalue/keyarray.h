#pragma once

#include "estl/h_vector.h"

namespace reindexer {

template <typename R, int holdSize>
class KeyArray : public h_vector<R, holdSize> {
public:
	using h_vector<R, holdSize>::h_vector;
	bool operator==(const KeyArray &other) const { return EQ(other); }
	bool operator!=(const KeyArray &other) const { return !EQ(other); }
	bool EQ(const KeyArray &other) const {
		if (other.size() != this->size()) return false;
		for (size_t i = 0; i < this->size(); ++i)
			if (this->at(i) != other.at(i)) return false;
		return true;
	}
	size_t Hash() const {
		size_t ret = this->size();
		for (size_t i = 0; i < this->size(); ++i) ret ^= this->at(i).Hash();
		return ret;
	}
};

}  // namespace reindexer
