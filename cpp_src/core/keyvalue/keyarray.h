#pragma once

#include "estl/h_vector.h"

namespace reindexer {

template <typename R, int holdSize>
class KeyArray : public h_vector<R, holdSize> {
public:
	using h_vector<R, holdSize>::h_vector;
	using h_vector<R, holdSize>::operator==;
	using h_vector<R, holdSize>::operator!=;
	size_t Hash() const {
		size_t ret = this->size();
		for (size_t i = 0; i < this->size(); ++i) ret ^= this->at(i).Hash();
		return ret;
	}
};

}  // namespace reindexer
