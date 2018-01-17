#pragma once

#if 1
#include "hopscotch/hopscotch_map.h"

namespace reindexer {
template <typename K, typename E, typename H = std::hash<K>, typename P = std::equal_to<K>>
using fast_hash_map = tsl::hopscotch_map<K, E, H, P>;
}

#else
#include <unordered_map>
namespace reindexer {
template <typename K, typename E, typename H = std::hash<K>, typename P = std::equal_to<K>>
using fast_hash_map = std::unordered_map<K, E, H, P>;
}
#endif
