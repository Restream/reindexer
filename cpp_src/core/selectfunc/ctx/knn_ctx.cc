#include "knn_ctx.h"
#include <span>

namespace reindexer {

void KnnCtx::Add(std::span<float> dists) {
	assertrx_dbg(ranks_.empty());
	ranks_.reserve(dists.size());
	for (float d : dists) {
		ranks_.push_back(d);
	}
}

void KnnCtx::Add(std::vector<RankT>&& dists) {
	assertrx_dbg(ranks_.empty());
	ranks_ = std::move(dists);
}

}  // namespace reindexer
