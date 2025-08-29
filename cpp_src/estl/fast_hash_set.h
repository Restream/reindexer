#pragma once

#if 1
#include "hopscotch/hopscotch_sc_set.h"
#include "hopscotch/hopscotch_set.h"

namespace reindexer {

template <typename K, typename H, typename P>
using hs_set_prime = tsl::hopscotch_set<K, H, P, std::allocator<K>, 62, false, tsl::prime_growth_policy>;
template <typename K, typename H, typename P, typename L>
using hs_sc_set_prime = tsl::hopscotch_sc_set<K, H, P, L, std::allocator<K>, 62, false, tsl::prime_growth_policy>;

// fast_hash_set_l is hopscotch set with estl::elist as overflow container. It has very bad worst case scenario due to linear complexity.
// May still be used, when you're sure about collisions count and don't want to implements 'less' functor.
// Uses prime_growth_policy for resizing
template <typename K, typename H = void, typename P = void>
class [[nodiscard]] fast_hash_set_l : public hs_set_prime<K, H, P> {
public:
	static_assert(!std::is_same_v<H, void>, "Hash functor must be specialized exlicitly");
	static_assert(!std::is_same_v<P, void>, "EqualTo functor must be specialized exlicitly");
	using Base = hs_set_prime<K, H, P>;
	using Base::Base;
	using const_iterator = typename Base::const_iterator;
	using iterator = typename Base::iterator;

	template <typename L>
	explicit fast_hash_set_l(typename Base::size_type bucket_count, const H& hash, const P& equal, const L& comp)
		: Base(bucket_count, hash, equal) {
		(void)comp;	 // constrcutor for compatibility with fast_hash_set_s
	}
};

template <typename K>
class [[nodiscard]] fast_hash_set_l<K> : public fast_hash_set_l<K, std::hash<K>, std::equal_to<K>> {
public:
	using Base = fast_hash_set_l<K, std::hash<K>, std::equal_to<K>>;
	using Base::Base;
	using const_iterator = typename Base::const_iterator;
	using iterator = typename Base::iterator;
};

template <typename K, typename H>
class [[nodiscard]] fast_hash_set_l<K, H> : public fast_hash_set_l<K, H, std::equal_to<K>> {
public:
	using Base = fast_hash_set_l<K, H, std::equal_to<K>>;
	using Base::Base;
	using const_iterator = typename Base::const_iterator;
	using iterator = typename Base::iterator;
};

// fast_hash_map_s is hopscotch map with std::set as overflow container. It has better performance in case of bad hash function than
// fast_hash_map_l.
// Uses prime_growth_policy for resizing
template <typename K, typename H = void, typename P = void, typename L = void>
class [[nodiscard]] fast_hash_set_s : public hs_sc_set_prime<K, H, P, L> {
public:
	static_assert(!std::is_same_v<H, void>, "Hash functor must be specialized exlicitly");
	static_assert(!std::is_same_v<P, void>, "EqualTo functor must be specialized exlicitly");
	static_assert(!std::is_same_v<L, void>, "Less functor must be specialized exlicitly");
	using Base = hs_sc_set_prime<K, H, P, L>;
	using Base::Base;
	using const_iterator = typename Base::const_iterator;
	using iterator = typename Base::const_iterator;	 // hs_sc_set_prime does not have non-const iterator

	explicit fast_hash_set_s(typename Base::size_type bucket_count, const H& hash, const P& equal, const L& comp)
		: Base(bucket_count, hash, equal, std::allocator<K>(), comp) {}
};

template <typename K>
class [[nodiscard]] fast_hash_set_s<K> : public fast_hash_set_s<K, std::hash<K>, std::equal_to<K>, std::less<K>> {
public:
	using Base = fast_hash_set_s<K, std::hash<K>, std::equal_to<K>, std::less<K>>;
	using Base::Base;
	using const_iterator = typename Base::const_iterator;
};

template <typename K, typename H>
class [[nodiscard]] fast_hash_set_s<K, H> : public fast_hash_set_s<K, H, std::equal_to<K>, std::less<K>> {
public:
	using Base = fast_hash_set_s<K, H, std::equal_to<K>, std::less<K>>;
	using Base::Base;
	using const_iterator = typename Base::const_iterator;
};

// fast_hash_set uses fast_hash_set_s as default type for hash_sets
template <typename... Args>
using fast_hash_set = fast_hash_set_s<Args...>;

constexpr bool kUsingSTDFastHashSet = false;

}  // namespace reindexer

#else
#include <unordered_set>
namespace reindexer {
// This code will not compile with C++17, due to heterogenous for unordered containers was added in C++20
template <typename K, typename H = std::hash<K>, typename P = std::equal_to<K>, typename L = void>
using fast_hash_set = std::unordered_set<K, H, P>;

template <typename K, typename H = std::hash<K>, typename P = std::equal_to<K>, typename L = void>
using fast_hash_set_s = std::unordered_set<K, H, P>;

template <typename K, typename H = std::hash<K>, typename P = std::equal_to<K>, typename L = void>
using fast_hash_set_l = std::unordered_set<K, H, P>;

constexpr bool kUsingSTDFastHashSet = true;

}  // namespace reindexer
#endif
