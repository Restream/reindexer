#pragma once

#include "basefunctionctx.h"
#include "core/enums.h"

namespace reindexer {

class KnnCtx : public BaseFunctionCtx {
public:
	KnnCtx() noexcept : BaseFunctionCtx{CtxType::kKnnCtx} {}
	RankT Rank(size_t pos) const noexcept override {
		assertrx_dbg(pos < ranks_.size());
		return ranks_[pos];
	}
	void Add(std::span<float>);
	void Add(std::vector<RankT>&&);
	void NeedSort(reindexer::NeedSort needSort) noexcept { needSort_ = needSort; }
	reindexer::NeedSort NeedSort() const noexcept { return needSort_; }

private:
	std::vector<RankT> ranks_;
	reindexer::NeedSort needSort_{NeedSort_True};
};

}  // namespace reindexer
