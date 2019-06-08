#pragma once

#include <unordered_map>
#include "core/payload/fieldsset.h"
#include "core/payload/payloadiface.h"
#include "cpp-btree/btree_map.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

struct equal_composite {
	equal_composite(const PayloadType type, const FieldsSet &fields) : type_(type), fields_(fields) {}
	bool operator()(const PayloadValue &lhs, const PayloadValue &rhs) const {
		assert(type_);
		return ConstPayload(type_, lhs).IsEQ(rhs, fields_);
	}
	PayloadType type_;
	FieldsSet fields_;
};
struct hash_composite {
	hash_composite(const PayloadType type, const FieldsSet &fields) : type_(type), fields_(fields) {}
	size_t operator()(const PayloadValue &s) const {
		assert(type_);
		return ConstPayload(type_, s).GetHash(fields_);
	}
	PayloadType type_;
	FieldsSet fields_;
};

struct less_composite {
	less_composite(const PayloadType type, const FieldsSet &fields) : type_(type), fields_(fields) {}
	bool operator()(const PayloadValue &lhs, const PayloadValue &rhs) const {
		assert(type_);
		assert(!lhs.IsFree());
		assert(!rhs.IsFree());
		return (ConstPayload(type_, lhs).Compare(rhs, fields_) < 0);
	}
	PayloadType type_;
	FieldsSet fields_;
};

template <typename T1>
class unordered_payload_map : public std::unordered_map<PayloadValue, T1, hash_composite, equal_composite> {
public:
	using base_hash_map = std::unordered_map<PayloadValue, T1, hash_composite, equal_composite>;
	using base_hash_map::base_hash_map;
	unordered_payload_map(const PayloadType payloadType, const FieldsSet &fields, const CollateOpts)
		: base_hash_map(1000, hash_composite(payloadType, fields), equal_composite(payloadType, fields)) {}
};

template <typename T1>
class payload_map : public btree::btree_map<PayloadValue, T1, less_composite> {
public:
	using base_tree_map = btree::btree_map<PayloadValue, T1, less_composite>;
	using base_tree_map::base_tree_map;
	payload_map(const PayloadType payloadType, const FieldsSet &fields, const CollateOpts)
		: base_tree_map(less_composite(payloadType, fields)) {}
};

using unordered_payload_set = fast_hash_set<PayloadValue, hash_composite, equal_composite>;

}  // namespace reindexer
