#pragma once

#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include "core/payload/fieldsset.h"
#include "core/payload/payloadiface.h"
#include "cpp-btree/btree_map.h"

namespace reindexer {

using btree::btree_map;
using std::unordered_map;
using std::unordered_set;

struct equal_composite {
	equal_composite(const PayloadType type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	bool operator()(const PayloadValue &lhs, const PayloadValue &rhs) const {
		assert(type_);
		return ConstPayload(type_, lhs).IsEQ(rhs, fields_);
	}
	PayloadType type_;
	FieldsSet fields_;

	// Required only for Index<unordered_payload_map> template instantion. Should not be called
	equal_composite() { abort(); }
};
struct hash_composite {
	hash_composite(const PayloadType type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	size_t operator()(const PayloadValue &s) const {
		assert(type_);
		return ConstPayload(type_, s).GetHash(fields_);
	}
	PayloadType type_;
	FieldsSet fields_;

	// Required only for Index<unordered_payload_map> template instantion. Should not be called
	hash_composite() { abort(); };
};

struct less_composite {
	less_composite(const PayloadType type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	bool operator()(const PayloadValue &lhs, const PayloadValue &rhs) const {
		assert(type_);
		assert(!lhs.IsFree());
		assert(!rhs.IsFree());
		return (ConstPayload(type_, lhs).Compare(rhs, fields_) < 0);
	}
	PayloadType type_;
	FieldsSet fields_;
	// Required only for Index<payload_map> template instantion. Should not be called
	less_composite() { abort(); };
};

template <typename T1>
using unordered_payload_map = unordered_map<PayloadValue, T1, hash_composite, equal_composite>;
template <typename T1>
using payload_map = btree_map<PayloadValue, T1, less_composite>;
using unordered_payload_set = unordered_set<PayloadValue, hash_composite, equal_composite>;

template <typename T>
struct is_payload_unord_map_key : std::false_type {};
template <typename T1>
struct is_payload_unord_map_key<unordered_payload_map<T1>> : std::true_type {};
template <typename T>
struct is_payload_map_key : std::false_type {};
template <typename T1>
struct is_payload_map_key<payload_map<T1>> : std::true_type {};

}  // namespace reindexer
