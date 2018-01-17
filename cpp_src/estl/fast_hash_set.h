#pragma once

#if 1
#include "hopscotch/hopscotch_set.h"

namespace reindexer {
template <typename K, typename H = std::hash<K>, typename P = std::equal_to<K>>
using fast_hash_set = tsl::hopscotch_set<K, H, P>;
}

#else
#include <unordered_set>
namespace reindexer {
template <typename K, typename H = std::hash<K>, typename P = std::equal_to<K>>
using fast_hash_set = std::unordered_set<K, H, P>;
}
#endif
