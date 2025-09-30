#pragma once

#include "core/keyvalue/key_string.h"
#include "core/keyvalue/uuid.h"
#include "core/namespace/stringsholder.h"
#include "cpp-btree/btree_map.h"
#include "estl/sparse_hash_int.h"
#include "sparse-map/sparse_map.h"
#include "tools/stringstools.h"

namespace reindexer {

struct [[nodiscard]] less_key_string {
	using is_transparent = void;

	less_key_string(const CollateOpts& collateOpts = CollateOpts()) : collateOpts_(collateOpts) {}
	bool operator()(const key_string& lhs, const key_string& rhs) const noexcept {
		return collateCompare(lhs, rhs, collateOpts_) == ComparationResult::Lt;
	}
	bool operator()(std::string_view lhs, const key_string& rhs) const noexcept {
		return collateCompare(lhs, rhs, collateOpts_) == ComparationResult::Lt;
	}
	bool operator()(const key_string& lhs, std::string_view rhs) const noexcept {
		return collateCompare(lhs, rhs, collateOpts_) == ComparationResult::Lt;
	}
	CollateOpts collateOpts_;
};

class [[nodiscard]] key_string_with_hash : public key_string {
public:
	key_string_with_hash() noexcept : key_string() {}
	key_string_with_hash(key_string s, CollateMode cm)
		: key_string(std::move(s)), hash_(collateHash(*static_cast<key_string*>(this), cm)) {}
	key_string_with_hash(const key_string_with_hash& o) noexcept : key_string(o), hash_(o.hash_) {}
	key_string_with_hash(key_string_with_hash&& o) noexcept : key_string(std::move(o)), hash_(o.hash_) {}
	key_string_with_hash& operator=(key_string_with_hash&& o) noexcept {
		hash_ = o.hash_;
		return static_cast<key_string_with_hash&>(key_string::operator=(std::move(o)));
	}
	uint32_t GetHash() const noexcept { return hash_; }

private:
	uint32_t hash_ = 0;
};

struct [[nodiscard]] equal_key_string {
	using is_transparent = void;

	equal_key_string(const CollateOpts& collateOpts = CollateOpts()) : collateOpts_(collateOpts) {}
	bool operator()(const key_string& lhs, const key_string& rhs) const noexcept {
		return collateCompare(lhs, rhs, collateOpts_) == ComparationResult::Eq;
	}
	bool operator()(std::string_view lhs, const key_string& rhs) const noexcept {
		return collateCompare(lhs, rhs, collateOpts_) == ComparationResult::Eq;
	}
	bool operator()(const key_string& lhs, std::string_view rhs) const noexcept {
		return collateCompare(lhs, rhs, collateOpts_) == ComparationResult::Eq;
	}

private:
	CollateOpts collateOpts_;
};

struct [[nodiscard]] hash_key_string {
	using is_transparent = void;

	hash_key_string(CollateMode collateMode = CollateNone) noexcept : collateMode_(collateMode) {}
	size_t operator()(const key_string& s) const noexcept { return collateHash(s, collateMode_); }
	size_t operator()(std::string_view s) const noexcept { return collateHash(s, collateMode_); }
	size_t operator()(const key_string_with_hash& s) const noexcept { return s.GetHash(); }

private:
	CollateMode collateMode_;
};

template <typename T1>
class unordered_str_map
	: public tsl::sparse_map<key_string_with_hash, T1, hash_key_string, equal_key_string,
							 std::allocator<std::pair<key_string_with_hash, T1>>, tsl::sh::power_of_two_growth_policy<2>,
							 tsl::sh::exception_safety::basic, tsl::sh::sparsity::low> {
	using base_hash_map =
		tsl::sparse_map<key_string_with_hash, T1, hash_key_string, equal_key_string, std::allocator<std::pair<key_string_with_hash, T1>>,
						tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

public:
	static_assert(std::is_nothrow_move_constructible<std::pair<key_string, T1>>::value, "Nothrow movebale key and value required");
	using typename base_hash_map::iterator;
	unordered_str_map() : base_hash_map() {}
	unordered_str_map(const CollateOpts& opts)
		: base_hash_map(1000, hash_key_string(CollateMode(opts.mode)), equal_key_string(opts)), collateMode_(opts.mode) {}

	template <typename deep_cleaner>
	iterator erase(iterator pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_hash_map::erase(pos);
	}

	template <typename deep_cleaner>
	iterator erase(iterator pos, const deep_cleaner& deep_clean) {
		deep_clean(*pos);
		return base_hash_map::erase(pos);
	}

	std::pair<iterator, bool> insert(const std::pair<key_string, T1>& v) {
		key_string_with_hash key(v.first, collateMode_);
		return base_hash_map::insert(std::make_pair(std::move(key), v.second));
	}
	std::pair<iterator, bool> insert(std::pair<key_string, T1>&& v) {
		key_string_with_hash key(std::move(v.first), collateMode_);
		return base_hash_map::insert(std::make_pair(std::move(key), std::move(v.second)));
	}
	std::pair<iterator, bool> emplace(key_string k, T1&& v) {
		key_string_with_hash key(std::move(k), collateMode_);
		return base_hash_map::emplace(std::make_pair(std::move(key), std::move(v)));
	}

private:
	const CollateMode collateMode_ = CollateNone;
};

template <typename T1>
class [[nodiscard]] str_map : public btree::btree_map<key_string, T1, less_key_string> {
	using base_tree_map = btree::btree_map<key_string, T1, less_key_string>;

public:
	using typename base_tree_map::iterator;
	str_map(const CollateOpts& opts) : base_tree_map(less_key_string(opts)) {}

	template <typename deep_cleaner>
	iterator erase(const iterator& pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_tree_map::erase(pos);
	}
	template <typename deep_cleaner>
	iterator erase(const iterator& pos, const deep_cleaner& deep_clean) {
		deep_clean(*pos);
		return base_tree_map::erase(pos);
	}
};

template <typename K, typename T>
class unordered_number_map
	: public tsl::sparse_map<K, T, hash_int<K>, std::equal_to<K>, std::allocator<std::pair<K, T>>, tsl::sh::power_of_two_growth_policy<2>,
							 tsl::sh::exception_safety::basic, tsl::sh::sparsity::low> {
	using base_hash_map = tsl::sparse_map<K, T, hash_int<K>, std::equal_to<K>, std::allocator<std::pair<K, T>>,
										  tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

public:
	using typename base_hash_map::iterator;
	unordered_number_map() = default;

	template <typename deep_cleaner>
	iterator erase(iterator pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_hash_map::erase(pos);
	}
};

struct [[nodiscard]] hash_uuid {
	size_t operator()(Uuid uuid) const noexcept {
		constexpr static hash_int<uint64_t> intHasher;
		return intHasher(uuid.data_[0]) ^ (intHasher(uuid.data_[1]) << 19) ^ (intHasher(uuid.data_[1]) >> 23);
	}
};

template <typename T>
class unordered_uuid_map
	: public tsl::sparse_map<Uuid, T, hash_uuid, std::equal_to<Uuid>, std::allocator<std::pair<Uuid, T>>,
							 tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low> {
	using base_hash_map = tsl::sparse_map<Uuid, T, hash_uuid, std::equal_to<Uuid>, std::allocator<std::pair<Uuid, T>>,
										  tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

public:
	using typename base_hash_map::iterator;
	unordered_uuid_map() = default;

	template <typename deep_cleaner>
	iterator erase(iterator pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_hash_map::erase(pos);
	}
};

template <typename K, typename T1>
class [[nodiscard]] number_map : public btree::btree_map<K, T1> {
	using base_tree_map = btree::btree_map<K, T1>;

public:
	number_map() = default;
	using typename base_tree_map::iterator;

	template <typename deep_cleaner>
	iterator erase(const iterator& pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_tree_map::erase(pos);
	}
};

template <bool deepClean>
struct [[nodiscard]] StringMapEntryCleaner {
	StringMapEntryCleaner(StringsHolder& strHolder, bool needSaveExpiredStrings) noexcept
		: strHolder_{strHolder}, needSaveExpiredStrings_{needSaveExpiredStrings} {}

	template <typename T>
	constexpr static bool RequiresStringHolder() noexcept {
		return std::is_same_v<std::remove_cv_t<T>, key_string_with_hash> || std::is_same_v<std::remove_cv_t<T>, key_string>;
	}

	template <typename T>
	void operator()(T& v) const {
		free_node(v.first);
		free_node(v.second);
	}

	template <typename T, std::enable_if_t<!RequiresStringHolder<T>(), bool> = true>
	void free_node(T& v) const {
		if constexpr (deepClean && !std::is_const_v<T>) {
			v = T{};
		}
	}

	template <typename T, std::enable_if_t<RequiresStringHolder<T>(), bool> = true>
	void free_node(const T& str) const {
		if (needSaveExpiredStrings_) {
			strHolder_.Add(str);
		}
	}

	template <typename T, std::enable_if_t<RequiresStringHolder<T>(), bool> = true>
	void free_node(T& str) const {
		if (needSaveExpiredStrings_) {
			strHolder_.Add(std::move(str));
		} else if constexpr (deepClean) {
			str = T{};
		}
	}

private:
	StringsHolder& strHolder_;
	const bool needSaveExpiredStrings_;
};

template <typename>
constexpr bool is_str_map_v = false;

template <typename T>
constexpr bool is_str_map_v<str_map<T>> = true;

template <typename T>
constexpr bool is_str_map_v<unordered_str_map<T>> = true;

}  // namespace reindexer
