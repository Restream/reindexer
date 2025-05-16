#pragma once

#include <span>
#include "core/enums.h"
#include "core/nsselecter/ranks_holder.h"

namespace reindexer {

class [[nodiscard]] KnnCtx {
public:
	KnnCtx(RanksHolder::Ptr r) noexcept : ranks_{std::move(r)} { assertrx_dbg(ranks_); }
	void Add(std::span<float> r) { ranks_->Add(r); }
	void Add(std::vector<RankT>&& r) noexcept { ranks_->Add(std::move(r)); }
	void NeedSort(reindexer::NeedSort needSort) noexcept { needSort_ = needSort; }
	reindexer::NeedSort NeedSort() const noexcept { return needSort_; }

private:
	RanksHolder::Ptr ranks_;
	reindexer::NeedSort needSort_{NeedSort_True};
};

}  // namespace reindexer
