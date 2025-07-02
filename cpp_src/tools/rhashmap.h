#pragma once
// #define REINDEX_FT_EXTRA_DEBUG

#ifdef REINDEX_FT_EXTRA_DEBUG
#include <unordered_map>
#endif	// REINDEX_FT_EXTRA_DEBUG
#include "estl/fast_hash_map.h"

namespace reindexer {
#ifdef REINDEX_FT_EXTRA_DEBUG
template <typename K, typename V, typename HashT = std::hash<K>, typename EqualT = std::equal_to<K>, typename LessT = std::less<K>>
using RHashMap = std::unordered_map<K, V, HashT, EqualT>;
#else
template <typename K, typename V, typename HashT = std::hash<K>, typename EqualT = std::equal_to<K>, typename LessT = std::less<K>>
using RHashMap = fast_hash_map<K, V, HashT, EqualT, LessT>;
#endif
}  // namespace reindexer
