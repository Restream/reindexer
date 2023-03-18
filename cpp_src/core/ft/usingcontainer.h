#pragma once
#include <vector>
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"

// #define REINDEX_FT_EXTRA_DEBUG

namespace reindexer {

#ifdef REINDEX_FT_EXTRA_DEBUG
template <typename T, int holdSize = 4>
using RVector = std::vector<T>;
#else
template <typename T, int holdSize = 4>
using RVector = h_vector<T, holdSize>;
#endif

#ifdef REINDEX_FT_EXTRA_DEBUG
template <typename K, typename V>
using RHashMap = std::unordered_map<K, V>;
#else
template <typename K, typename V>
using RHashMap = fast_hash_map<K, V>;
#endif
}  // namespace reindexer