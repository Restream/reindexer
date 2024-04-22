#include "stringsholder.h"
#include "core/index/index.h"

namespace reindexer {

StringsHolder::~StringsHolder() = default;

void StringsHolder::Add(std::unique_ptr<Index>&& idx) {
	idx->DestroyCache();
	indexes_.emplace_back(std::move(idx));
}

void StringsHolder::Clear() noexcept {
	Base::clear();
	indexes_.clear();
	memStat_ = 0;
}

StringsHolderPtr makeStringsHolder() { return StringsHolderPtr{new intrusive_atomic_rc_wrapper<StringsHolder>}; }

}  // namespace reindexer
