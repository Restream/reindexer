#pragma once

#if 1
#include "fast_hash_traits.h"
#include "vendor/hopscotch/hopscotch_map.h"
#include "vendor/hopscotch/hopscotch_sc_map.h"

namespace reindexer {

template <typename K, typename E, typename H, typename P>
using hs_map_prime = tsl::hopscotch_map<K, E, H, P, std::allocator<std::pair<K, E>>, 62, false, tsl::prime_growth_policy>;
template <typename K, typename E, typename H, typename P, typename L>
using hs_sc_map_prime = tsl::hopscotch_sc_map<K, E, H, P, L, std::allocator<std::pair<const K, E>>, 62, false, tsl::prime_growth_policy>;

// fast_hash_map_s is hopscotch map with estl::elist as overflow container. It has very bad worst case scenario due to linear complexity,
// but is's able to get advantage from moving keys and fast_hash_map_s don't.
// Uses prime_growth_policy for resizing.
template <typename K, typename E, typename H = void, typename P = void>
class [[nodiscard]] fast_hash_map_l : public hs_map_prime<K, E, H, P> {
public:
	static_assert(!std::is_same_v<H, void>, "Hash functor must be specialized exlicitly");
	static_assert(!std::is_same_v<P, void>, "EqualTo functor must be specialized exlicitly");
	using Base = hs_map_prime<K, E, H, P>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;

	template <typename L>
	explicit fast_hash_map_l(typename Base::size_type bucket_count, const H& hash, const P& equal, const L& comp)
		: Base(bucket_count, hash, equal) {
		(void)comp;	 // constrcutor for compatibility with fast_hash_map_s
	}
};

template <typename K, typename E>
class [[nodiscard]] fast_hash_map_l<K, E> : public fast_hash_map_l<K, E, std::hash<K>, std::equal_to<K>> {
public:
	using Base = fast_hash_map_l<K, E, std::hash<K>, std::equal_to<K>>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;
};

template <typename K, typename E, typename H>
class [[nodiscard]] fast_hash_map_l<K, E, H> : public fast_hash_map_l<K, E, H, std::equal_to<K>> {
public:
	using Base = fast_hash_map_l<K, E, H, std::equal_to<K>>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;
};

// fast_hash_map_s is hopscotch map with std::set as overflow container. Due to internal specific, sometimes it has to copy keys instead of
// moving it, however in works better with a bad hash-function, than fast_hash_map_l.
// Uses prime_growth_policy for resizing
template <typename K, typename E, typename H = void, typename P = void, typename L = void>
class [[nodiscard]] fast_hash_map_s : public hs_sc_map_prime<K, E, H, P, L> {
public:
	static_assert(!std::is_same_v<H, void>, "Hash functor must be specialized exlicitly");
	static_assert(!std::is_same_v<P, void>, "EqualTo functor must be specialized exlicitly");
	static_assert(!std::is_same_v<L, void>, "Less functor must be specialized exlicitly");
	using Base = hs_sc_map_prime<K, E, H, P, L>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;

	explicit fast_hash_map_s(typename Base::size_type bucket_count, const H& hash, const P& equal, const L& comp)
		: Base(bucket_count, hash, equal, std::allocator<K>(), comp) {}
};

template <typename K, typename E>
class [[nodiscard]] fast_hash_map_s<K, E> : public fast_hash_map_s<K, E, std::hash<K>, std::equal_to<K>, std::less<K>> {
public:
	using Base = fast_hash_map_s<K, E, std::hash<K>, std::equal_to<K>, std::less<K>>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;
};

template <typename K, typename E, typename H>
class [[nodiscard]] fast_hash_map_s<K, E, H> : public fast_hash_map_s<K, E, H, std::equal_to<K>, std::less<K>> {
public:
	using Base = fast_hash_map_s<K, E, H, std::equal_to<K>, std::less<K>>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;
};

// fast_hash_map attempts to choose between fast_hash_map_s and fast_hash_map_l depending on Key(K) type and is_using_sc_version_v trait
template <typename K, typename E, typename H = void, typename P = void, typename L = void>
class [[nodiscard]] fast_hash_map
	: public std::conditional_t<is_using_sc_version_v<K>, fast_hash_map_s<K, E, H, P, L>, fast_hash_map_l<K, E, H, P>> {
public:
	using Base = std::conditional_t<is_using_sc_version_v<K>, fast_hash_map_s<K, E, H, P, L>, fast_hash_map_l<K, E, H, P>>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;
};

template <typename K, typename E>
class [[nodiscard]] fast_hash_map<K, E> : public fast_hash_map<K, E, std::hash<K>, std::equal_to<K>, std::less<K>> {
public:
	using Base = fast_hash_map<K, E, std::hash<K>, std::equal_to<K>, std::less<K>>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;
};

template <typename K, typename E, typename H>
class [[nodiscard]] fast_hash_map<K, E, H> : public fast_hash_map<K, E, H, std::equal_to<K>, std::less<K>> {
public:
	using Base = fast_hash_map<K, E, H, std::equal_to<K>, std::less<K>>;
	using Base::Base;
	using iterator = typename Base::iterator;
	using const_iterator = typename Base::const_iterator;
};

constexpr bool kUsingSTDFastHashMap = false;

}  // namespace reindexer

#else
#include <unordered_map>
namespace reindexer {
// This code will not compile with C++17, due to heterogenous for unordered containers was added in C++20
template <typename K, typename E, typename H = std::hash<K>, typename P = std::equal_to<K>, typename L = void>
using fast_hash_map = std::unordered_map<K, E, H, P>;

template <typename K, typename E, typename H = std::hash<K>, typename P = std::equal_to<K>, typename L = void>
using fast_hash_map_s = std::unordered_map<K, E, H, P>;

template <typename K, typename E, typename H = std::hash<K>, typename P = std::equal_to<K>, typename L = void>
using fast_hash_map_l = std::unordered_map<K, E, H, P>;

constexpr bool kUsingSTDFastHashMap = true;

}  // namespace reindexer
#endif
