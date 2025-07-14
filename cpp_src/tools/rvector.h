#pragma once

// #define REINDEX_FT_EXTRA_DEBUG

#ifdef REINDEX_FT_EXTRA_DEBUG
#include <vector>
#endif	// REINDEX_FT_EXTRA_DEBUG
#include "estl/h_vector.h"

namespace reindexer {

#ifdef REINDEX_FT_EXTRA_DEBUG
template <typename T, int holdSize = 4>
class RVector : public std::vector<T> {
public:
	typedef unsigned size_type;

	using std::vector<T>::vector;

	[[nodiscard]] bool operator==(const RVector& other) const = default;

	template <bool F = true>
	void clear() noexcept {
		std::vector<T>::clear();
	}

	[[nodiscard]] size_t heap_size() { return std::vector<T>::capacity() * sizeof(T); }

	[[nodiscard]] static constexpr size_type max_size() noexcept { return std::numeric_limits<size_type>::max() >> 1; }

	void grow(size_type sz) {
		size_type cap = std::vector<T>::capacity();
		if (sz > cap) {
			std::vector<T>::reserve(std::max(sz, std::min(max_size(), cap * 2)));
		}
	}
};

#else
template <typename T, int holdSize = 4>
class RVector : public h_vector<T, holdSize> {
public:
	using h_vector<T, holdSize>::h_vector;
};
#endif

}  // namespace reindexer
