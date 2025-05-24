#pragma once

#include <span>
#include "core/rank_t.h"
#include "estl/intrusive_ptr.h"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] RanksHolder : public intrusive_atomic_rc_base {
public:
	using Ptr = intrusive_ptr<RanksHolder>;

	RankT Get(size_t pos) const noexcept { return pos < ranks_.size() ? ranks_[pos] : RankT{}; }
	void Set(size_t pos, RankT r) noexcept {
		assertrx_dbg(pos < ranks_.size());
		ranks_[pos] = r;
	}

	void Add(RankT r) { ranks_.emplace_back(r); }
	void Add(std::span<float> dists) {
		assertrx_dbg(ranks_.empty());
		ranks_.reserve(dists.size());
		for (float d : dists) {
			ranks_.emplace_back(d);
		}
	}
	void Add(std::vector<RankT>&& dists) noexcept {
		assertrx_dbg(ranks_.empty());
		ranks_ = std::move(dists);
	}
	void Reserve(size_t size) { ranks_.reserve(size); }
	[[nodiscard]] size_t Size() const noexcept { return ranks_.size(); }
	[[nodiscard]] bool Empty() const noexcept { return ranks_.empty(); }

private:
	std::vector<RankT> ranks_;
};

}  // namespace reindexer
