#include "basemerger.h"
#include <memory>
#include <vector>
#include "core/ft/ft_fuzzy/merger/dataset.h"
#include "core/rdxcontext.h"
#include "estl/fast_hash_map.h"
#include "sort/pdqsort.hpp"
#include "tools/float_comparison.h"

namespace search_engine {

double bound(double k, double weight, double boost) { return (1.0 - weight) + k * boost * weight; }

void MergedData::Add(const IDCtx& ctx) {
	double max_dist = 0;
	size_t curent = 0;
	double max_src_dist = ctx.cfg.maxSrcProc;
	double max_dst_dist = ctx.cfg.maxDstProc;
	for (size_t i = 0; i < ctx.data->size(); ++i) {
		size_t size = 1;
		auto it = size_it_->second.find(ctx.data->at(i).field());
		if (it != size_it_->second.end()) {
			size = it->second;
		}
		double src_dst = abs(static_cast<int>(ctx.data->at(i).pos()) - ctx.pos);
		if (src_dst > ctx.total_size) {
			src_dst = ctx.total_size - 1;
		}
		src_dst = (ctx.total_size * ctx.cfg.posSourceBoost - src_dst) / double(ctx.total_size * ctx.cfg.posSourceBoost);
		double coof = 1;
		if (!first_) {
			coof = ctx.pos - prev_.src_pos;

			if (src_dst < 0) {
				coof = ctx.cfg.posSourceDistMin;
			} else {
				coof = (ctx.total_size * ctx.cfg.posSourceDistBoost - (coof - 1)) / double(ctx.total_size * ctx.cfg.posSourceDistBoost);
			}
		}
		src_dst *= coof;
		src_dst = (src_dst * max_src_dist) / ctx.total_size;
		double dst = 1;

		if (!first_) {
			dst = abs(static_cast<int>(ctx.data->at(i).pos()) - prev_.pos) - 1;

			dst = (ctx.total_size * ctx.cfg.posDstBoost - dst) / (double(ctx.total_size) * ctx.cfg.posDstBoost);
		}

		dst = (dst * max_dst_dist) / size;

		double fboost = ctx.opts->fieldsOpts[ctx.data->at(i).field()].boost;
		if (reindexer::fp::IsZero(fboost)) {
			fboost = 1;
		}

		src_dst = src_dst + dst;
		src_dst *= fboost;

		if (src_dst > max_dist) {
			max_dist = src_dst;
			curent = i;
		}
	}

	rank_ = max_dist * double(ctx.proc);
	if (!first_) {
		rank_ += prev_.proc;
	}
	if (rank_ > *ctx.max_proc) {
		*ctx.max_proc = rank_;
	}
	count_++;
	prev_ = ResultMerger{ctx.pos, 0, static_cast<int>(ctx.data->at(curent).pos()), rank_};

	first_ = false;
}

BaseMerger::BaseMerger(int max_id, int min_id) : max_id_(max_id), min_id_(min_id) {}

SearchResult BaseMerger::Merge(MergeCtx& ctx, bool inTransaction, const reindexer::RdxContext& rdxCtx) const {
	SearchResult result;
	result.data_ = std::make_shared<std::vector<MergedData>>();
	result.max_proc_ = 0;
	if (min_id_ > max_id_) {
		return result;
	}
	DataSet<MergedData> data_set(min_id_, max_id_);
	double max_proc = 0;
	for (auto& res : *ctx.results) {
		if (!inTransaction) {
			ThrowOnCancel(rdxCtx);
		}
		for (auto it = res.data->begin(); it != res.data->end(); ++it) {
			IDCtx id_ctx{&it->Pos(), res.pos, &max_proc, ctx.total_size, res.opts, *ctx.cfg, res.proc, ctx.sizes};

			data_set.AddData(it->Id(), id_ctx);
		}
	}
	boost::sort::pdqsort(data_set.data_->begin(), data_set.data_->end(), [](const MergedData& lhs, const MergedData& rhs) noexcept {
		if (reindexer::fp::ExactlyEqual(lhs.rank_, rhs.rank_)) {
			return lhs.id_ < rhs.id_;
		}
		return lhs.rank_ > rhs.rank_;
	});

	return SearchResult{data_set.data_, max_proc};
}
}  // namespace search_engine
