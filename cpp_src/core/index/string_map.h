#pragma once

#include <unordered_map>
#include "core/keyvalue/key_string.h"
#include "cpp-btree/btree_map.h"
#include "estl/intrusive_ptr.h"
#include "tools/customhash.h"
#include "tools/customlocal.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {

using btree::btree_map;
using std::min;
using std::stoi;
using std::to_string;
using std::unordered_map;

struct comparator_sptr {
	comparator_sptr(const CollateOpts& collateOpts = CollateOpts()) : collateOpts_(collateOpts) {}
	bool operator()(const key_string& lhs, const key_string& rhs) const {
		return collateCompare(string_view(*lhs), string_view(*rhs), collateOpts_) < 0;
	}
	CollateOpts collateOpts_;
};  // namespace reindexer

struct equal_sptr {
	equal_sptr(const CollateOpts& collateOpts = CollateOpts()) : collateOpts_(collateOpts) {}
	bool operator()(const key_string& lhs, const key_string& rhs) const {
		return collateCompare(string_view(*lhs), string_view(*rhs), collateOpts_) == 0;
	}
	CollateOpts collateOpts_;
};
struct hash_sptr {
	hash_sptr(CollateMode collateMode = CollateNone) : collateMode_(collateMode) {}
	size_t operator()(const key_string& s) const { return collateHash(*s, collateMode_); }
	CollateMode collateMode_;
};

template <typename T1>
using unordered_str_map = unordered_map<key_string, T1, hash_sptr, equal_sptr>;
template <typename T1>
using str_map = btree_map<key_string, T1, comparator_sptr>;

template <typename T>
struct is_string_unord_map_key : std::false_type {};
template <typename T1>
struct is_string_unord_map_key<unordered_str_map<T1>> : std::true_type {};
template <typename T>
struct is_string_map_key : std::false_type {};
template <typename T1>
struct is_string_map_key<str_map<T1>> : std::true_type {};

}  // namespace reindexer
