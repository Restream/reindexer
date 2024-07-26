#pragma once
#include <vector>
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"

// #define REINDEX_FT_EXTRA_DEBUG

namespace reindexer {

#ifdef REINDEX_FT_EXTRA_DEBUG
template <typename T, int holdSize = 4>
class RVector : public std::vector<T> {
public:
	using std::vector<T>::vector;
	template <bool F = true>
	void clear() noexcept {
		std::vector<T>::clear();
	}
};
#else
template <typename T, int holdSize = 4>
class RVector : public h_vector<T, holdSize> {
public:
	using h_vector<T, holdSize>::h_vector;
};
#endif

#ifdef REINDEX_FT_EXTRA_DEBUG
template <typename K, typename V, typename HashT = std::hash<K>, typename EqualT = std::equal_to<K>, typename LessT = std::less<K> >
using RHashMap = std::unordered_map<K, V, HashT, EqualT>;
#else
template <typename K, typename V, typename HashT = std::hash<K>, typename EqualT = std::equal_to<K>, typename LessT = std::less<K> >
using RHashMap = fast_hash_map<K, V, HashT, EqualT, LessT>;
#endif
}  // namespace reindexer
