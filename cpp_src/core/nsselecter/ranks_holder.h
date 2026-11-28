#pragma once

#include <span>
#include "core/rank_t.h"
#include "estl/h_vector.h"
#include "estl/intrusive_ptr.h"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] RanksHolder : public intrusive_atomic_rc_base {
public:
	using Ptr = intrusive_ptr<RanksHolder>;
	struct [[nodiscard]] RankPos {
		RankT rank;
		size_t pos;
	};

	RankT GetRank(size_t i) { return i < ranks_.size() ? ranks_[i] : RankT{}; }
	RankPos GetRankPos(size_t i) const noexcept {
		assertrx_dbg(ranks_.size() == positions_.size());
		return i < ranks_.size() ? RankPos{ranks_[i], positions_[i]} : RankPos{RankT{}, 0};
	}
	void Set(size_t i, RankT r) noexcept {
		assertrx_dbg(i < ranks_.size());
		assertrx_dbg(positions_.empty());
		ranks_[i] = r;
	}
	void Set(size_t i, RankT r, size_t pos) noexcept {
		assertrx_dbg(i < ranks_.size());
		assertrx_dbg(ranks_.size() == positions_.size());
		ranks_[i] = r;
		positions_[i] = pos;
	}
	void Set(size_t i, RankPos rp) noexcept { return Set(i, rp.rank, rp.pos); }

	void Add(RankT r) {
		assertrx_dbg(positions_.empty());
		ranks_.emplace_back(r);
	}
	void Add(std::span<const float> dists) {
		assertrx_dbg(ranks_.empty());
		assertrx_dbg(positions_.empty());
		ranks_.reserve(dists.size());
		for (float d : dists) {
			ranks_.emplace_back(d);
		}
	}
	void Add(h_vector<RankT, 128>&& ranks) noexcept {
		assertrx_dbg(ranks_.empty());
		assertrx_dbg(positions_.empty());
		ranks_ = std::move(ranks);
	}
	void Reserve(size_t size) { ranks_.reserve(size); }
	size_t Size() const noexcept { return ranks_.size(); }
	bool Empty() const noexcept { return ranks_.empty(); }
	std::span<const RankT> GetRanksSpan() const& noexcept { return ranks_; }
	auto GetRanksSpan() const&& = delete;
	std::span<const size_t> GetPositionsSpan() const& noexcept { return positions_; }
	auto GetPositionsSpan() const&& = delete;
	void InitRRFPositions() {
		assertrx_throw(positions_.empty());
		if (ranks_.empty()) {
			return;
		}
		positions_.resize(ranks_.size());
		size_t pos = 1;
		RankT lastRank = ranks_.front();
		for (size_t i = 0, s = ranks_.size(); i < s; ++i) {
			if (ranks_[i] < lastRank) {
				lastRank = ranks_[i];
				pos = i + 1;
			}
			positions_[i] = pos;
		}
	}

private:
	h_vector<RankT, 128> ranks_;
	std::vector<size_t> positions_;
};

}  // namespace reindexer
