#pragma once

#include "core/keyvalue/key_string.h"
#include "core/payload/payloadtype.h"
#include "cpp-btree/btree_map.h"
#include "sparse-map/sparse_map.h"
#include "tools/stringstools.h"
namespace reindexer {

class FieldsSet;

struct less_key_string {
	using is_transparent = void;

	less_key_string(const CollateOpts& collateOpts = CollateOpts()) : collateOpts_(collateOpts) {}
	bool operator()(const key_string& lhs, const key_string& rhs) const { return collateCompare(*lhs, *rhs, collateOpts_) < 0; }
	bool operator()(string_view lhs, const key_string& rhs) const { return collateCompare(lhs, *rhs, collateOpts_) < 0; }
	bool operator()(const key_string& lhs, string_view rhs) const { return collateCompare(*lhs, rhs, collateOpts_) < 0; }
	CollateOpts collateOpts_;
};

struct equal_key_string {
	using is_transparent = void;

	equal_key_string(const CollateOpts& collateOpts = CollateOpts()) : collateOpts_(collateOpts) {}
	bool operator()(const key_string& lhs, const key_string& rhs) const { return collateCompare(*lhs, *rhs, collateOpts_) == 0; }
	bool operator()(string_view lhs, const key_string& rhs) const { return collateCompare(lhs, *rhs, collateOpts_) == 0; }
	bool operator()(const key_string& lhs, string_view rhs) const { return collateCompare(*lhs, rhs, collateOpts_) == 0; }
	CollateOpts collateOpts_;
};
struct hash_key_string {
	using is_transparent = void;

	hash_key_string(CollateMode collateMode = CollateNone) : collateMode_(collateMode) {}
	size_t operator()(const key_string& s) const { return collateHash(*s, collateMode_); }
	size_t operator()(string_view s) const { return collateHash(s, collateMode_); }
	CollateMode collateMode_;
};

template <typename T1>
class unordered_str_map
	: public tsl::sparse_map<key_string, T1, hash_key_string, equal_key_string, std::allocator<std::pair<key_string, T1>>,
							 tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low> {
	using base_hash_map = tsl::sparse_map<key_string, T1, hash_key_string, equal_key_string, std::allocator<std::pair<key_string, T1>>,
										  tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;
	using base_hash_map::erase;

public:
	static_assert(std::is_nothrow_move_constructible<std::pair<key_string, T1>>::value, "Nothrow movebale key and value required");
	using typename base_hash_map::iterator;
	unordered_str_map() : base_hash_map() {}
	unordered_str_map(const PayloadType, const FieldsSet&, const CollateOpts& opts)
		: base_hash_map(1000, hash_key_string(CollateMode(opts.mode)), equal_key_string(opts)) {}

	template <typename deep_cleaner>
	iterator erase(iterator pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_hash_map::erase(pos);
	}
};

template <typename T1>
class str_map : public btree::btree_map<key_string, T1, less_key_string> {
	using base_tree_map = btree::btree_map<key_string, T1, less_key_string>;
	using base_tree_map::erase;

public:
	using typename base_tree_map::iterator;
	str_map(const PayloadType, const FieldsSet&, const CollateOpts& opts) : base_tree_map(less_key_string(opts)) {}

	template <typename deep_cleaner>
	iterator erase(const iterator& pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_tree_map::erase(pos);
	}
};

// sparsemap meeds special hash for intergers, due to
// performance issue https://github.com/greg7mdp/sparsepp#integer-keys-and-other-hash-function-considerations
template <typename T>
struct hash_int {};

template <>
struct hash_int<int64_t> {
	size_t operator()(int64_t k) const { return (k ^ 14695981039346656037ULL) * 1099511628211ULL; }
};

template <>
struct hash_int<int32_t> {
	size_t operator()(int32_t k) const { return (k ^ 2166136261U) * 16777619UL; }
};

template <typename K, typename T1>
class unordered_number_map
	: public tsl::sparse_map<K, T1, hash_int<K>, std::equal_to<K>, std::allocator<std::pair<K, T1>>, tsl::sh::power_of_two_growth_policy<2>,
							 tsl::sh::exception_safety::basic, tsl::sh::sparsity::low> {
	using base_hash_map = tsl::sparse_map<K, T1, hash_int<K>, std::equal_to<K>, std::allocator<std::pair<K, T1>>,
										  tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;
	using base_hash_map::erase;

public:
	using typename base_hash_map::iterator;
	unordered_number_map(const PayloadType, const FieldsSet&, const CollateOpts&) {}

	template <typename deep_cleaner>
	iterator erase(iterator pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_hash_map::erase(pos);
	}
};

template <typename K, typename T1>
class number_map : public btree::btree_map<K, T1> {
	using base_tree_map = btree::btree_map<K, T1>;
	using base_tree_map::erase;

public:
	number_map(const PayloadType, const FieldsSet&, const CollateOpts&) {}
	using typename base_tree_map::iterator;

	template <typename deep_cleaner>
	iterator erase(const iterator& pos) {
		static const deep_cleaner deep_clean;
		deep_clean(*pos);
		return base_tree_map::erase(pos);
	}
};

}  // namespace reindexer
