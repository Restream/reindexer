#pragma once
// #define REINDEX_FT_EXTRA_DEBUG

#ifdef REINDEX_FT_EXTRA_DEBUG
#include <unordered_set>
#endif	// REINDEX_FT_EXTRA_DEBUG
#include "estl/fast_hash_set.h"

namespace reindexer {
#ifdef REINDEX_FT_EXTRA_DEBUG
template <typename K, typename HashT = std::hash<K>, typename EqualT = std::equal_to<K>, typename LessT = std::less<K>>
class [[nodiscard]] RSet : public std::unordered_set<K, HashT, EqualT> {
public:
	RSet(size_t b, const HashT& h, const EqualT& e, [[maybe_unused]] const LessT&) : std::unordered_set<K, HashT, EqualT>{b, h, e} {}
	using std::unordered_set<K, HashT, EqualT>::unordered_set;
};
#else
template <typename K, typename HashT = std::hash<K>, typename EqualT = std::equal_to<K>, typename LessT = std::less<K>>
using RSet = fast_hash_set<K, HashT, EqualT, LessT>;
#endif
}  // namespace reindexer
