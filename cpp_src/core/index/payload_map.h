#pragma once

#include <type_traits>
#include <unordered_map>
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadvalue.h"
#include "cpp-btree/btree_map.h"

namespace reindexer {

using btree::btree_map;
using std::unordered_map;

struct equal_composite {
	explicit equal_composite() : fields_() {}
	equal_composite(const PayloadType::Ptr type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	bool operator()(const PayloadValue &lhs, const PayloadValue &rhs) const {
		assert(type_);
		return ConstPayload(type_, lhs).IsEQ(&rhs, fields_);
	}
	PayloadType::Ptr type_ = nullptr;
	FieldsSet fields_;
};
struct hash_composite {
	explicit hash_composite() : fields_() {}
	hash_composite(const PayloadType::Ptr type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	size_t operator()(const PayloadValue &s) const {
		assert(type_);
		return ConstPayload(type_, s).GetHash(fields_);
	}
	PayloadType::Ptr type_ = nullptr;
	FieldsSet fields_;
};

struct less_composite {
	less_composite() : fields_() {}
	less_composite(const PayloadType::Ptr type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	bool operator()(const PayloadValue &lhs, const PayloadValue &rhs) const {
		assert(type_);
		assert(!lhs.IsFree());
		assert(!rhs.IsFree());
		return ConstPayload(type_, lhs).Less(&rhs, fields_);
	}
	PayloadType::Ptr type_ = nullptr;
	FieldsSet fields_;
};

template <typename T1>
using unordered_payload_map = unordered_map<PayloadValue, T1, hash_composite, equal_composite>;
template <typename T1>
using payload_map = btree_map<PayloadValue, T1, less_composite>;

template <typename T>
struct is_payload_unord_map_key : std::false_type {};
template <typename T1>
struct is_payload_unord_map_key<unordered_payload_map<T1>> : std::true_type {};
template <typename T>
struct is_payload_map_key : std::false_type {};
template <typename T1>
struct is_payload_map_key<payload_map<T1>> : std::true_type {};

}  // namespace reindexer
