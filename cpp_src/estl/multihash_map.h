#pragma once

#include "multihash_set.h"

namespace reindexer {

template <typename K, typename V, size_t N, typename H, typename C>
class [[nodiscard]] MultiHashMap : private MultiHashSetImpl<std::pair<K, V>, K, MultiHashMap<K, V, N, H, C>, N, H, C> {
	using Base = MultiHashSetImpl<std::pair<K, V>, K, MultiHashMap<K, V, N, H, C>, N, H, C>;

public:
	using Base::Base;
	using Base::emplace;
	using Base::empty;
	using Base::cbegin;
	using Base::cend;
	using Base::find;
	using Base::insert;
	using Base::size;
	using typename Base::Iterator;
	static const typename Base::KeyType& getKey(const typename Base::StoringType& v) noexcept { return v.first; }
};

}  // namespace reindexer
