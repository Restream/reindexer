#pragma once

#include <string_view>
#include <unordered_set>
#include "core/keyvalue/variant.h"

namespace reindexer {

class ComparatorNotIndexedDistinct {
public:
	ComparatorNotIndexedDistinct() : values_{make_intrusive<SetWrpType>()} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const Variant& v) const {
		assertrx_dbg(values_);
		return values_->find(v) == values_->cend();
	}
	void ExcludeValues(Variant&& v) {
		assertrx_dbg(values_);
		values_->emplace(std::move(v));
	}
	void ClearValues() noexcept {
		assertrx_dbg(values_);
		values_->clear();
	}

private:
	using SetType = std::unordered_set<Variant>;
	using SetWrpType = intrusive_rc_wrapper<SetType>;
	using SetPtrType = intrusive_ptr<SetWrpType>;

	SetPtrType values_;
};

}  // namespace reindexer
