#pragma once

#include "core/keyvalue/variant.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

class ComparatorNotIndexedDistinct {
public:
	ComparatorNotIndexedDistinct() = default;
	ComparatorNotIndexedDistinct(const ComparatorNotIndexedDistinct&) = default;
	ComparatorNotIndexedDistinct& operator=(const ComparatorNotIndexedDistinct&) = delete;
	ComparatorNotIndexedDistinct(ComparatorNotIndexedDistinct&&) = default;
	ComparatorNotIndexedDistinct& operator=(ComparatorNotIndexedDistinct&&) = default;

	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const Variant& v) const { return values_.find(v) == values_.cend(); }
	void ExcludeValues(Variant&& v) { values_.emplace(std::move(v)); }
	void ClearValues() noexcept { values_.clear(); }

private:
	using SetType = fast_hash_set<Variant>;
	SetType values_;
};

}  // namespace reindexer
