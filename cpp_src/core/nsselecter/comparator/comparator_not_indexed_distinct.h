#pragma once

#include "core/keyvalue/variant.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

class [[nodiscard]] ComparatorNotIndexedDistinct {
public:
	RX_ALWAYS_INLINE bool Compare(const Variant& v) const { return values_.find(v) == values_.cend(); }
	void ExcludeValues(Variant&& v) { values_.emplace(std::move(v)); }

private:
	using SetType = fast_hash_set<Variant>;
	SetType values_;
};

}  // namespace reindexer
