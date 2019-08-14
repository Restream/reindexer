#include "basemerger.h"
#include <iostream>
#include <memory>
#include <vector>
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "math.h"
#include "sort/pdqsort.hpp"

namespace search_engine {
using std::vector;
using std::pair;
using std::make_pair;

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
		double src_dst = abs(ctx.data->at(i).pos() - ctx.pos);
		if (src_dst > ctx.total_size) {
			src_dst = ctx.total_size - 1;
		}
		src_dst = (ctx.total_size * ctx.cfg.posSourceBoost - src_dst) / double(ctx.total_size * ctx.cfg.posSourceBoost);
		double coof = 1;
		if (!first_) {
			coof = ctx.pos - prev_.src_pos;

			if (src_dst < 0)
				coof = ctx.cfg.posSourceDistMin;
			else
				coof = (ctx.total_size * ctx.cfg.posSourceDistBoost - (coof - 1)) / double(ctx.total_size * ctx.cfg.posSourceDistBoost);
		}
		src_dst *= coof;
		src_dst = (src_dst * max_src_dist) / ctx.total_size;
		double dst = 1;

		if (!first_) {
			dst = abs(ctx.data->at(i).pos() - prev_.pos) - 1;

			dst = (ctx.total_size * ctx.cfg.posDstBoost - dst) / (double(ctx.total_size) * ctx.cfg.posDstBoost);
		}

		dst = (dst * max_dst_dist) / size;

		double fboost = ctx.opts->fieldsBoost[ctx.data->at(i).field()];
		if (!fboost) fboost = 1;

		src_dst = src_dst + dst;
		src_dst *= fboost;

		if (src_dst > max_dist) {
			max_dist = src_dst;
			curent = i;
		}
	}

	proc_ = max_dist * double(ctx.proc);
	if (!first_) {
		proc_ += prev_.proc;
	}
	if (proc_ > *ctx.max_proc) {
		*ctx.max_proc = proc_;
	}
	count_++;
	prev_ = ResultMerger{ctx.pos, 0, ctx.data->at(curent).pos(), proc_};

	first_ = false;
}

BaseMerger::BaseMerger(int max_id, int min_id) : max_id_(max_id), min_id_(min_id) {}

SearchResult BaseMerger::Merge(MergeCtx& ctx) {
	SearchResult res;
	res.data_ = std::make_shared<std::vector<MergedData>>();
	res.max_proc_ = 0;
	if (min_id_ > max_id_) {
		return res;
	}
	DataSet<MergedData> data_set(min_id_, max_id_);
	int pos = 0;
	double max_proc = 0;
	for (auto& res : *ctx.rusults) {
		for (auto it = res.data->begin(); it != res.data->end(); ++it) {
			IDCtx id_ctx{&it->pos, res.pos, &max_proc, ctx.total_size, res.opts, *ctx.cfg, res.proc, ctx.sizes};

			data_set.AddData(it->id, id_ctx);
			pos++;
		}
	}
	boost::sort::pdqsort(data_set.data_->begin(), data_set.data_->end(), [](const MergedData& lhs, const MergedData& rhs) {
		if (lhs.proc_ == rhs.proc_) {
			return lhs.id_ < rhs.id_;
		}
		return lhs.proc_ > rhs.proc_;
	});

	return SearchResult{data_set.data_, max_proc};
}
}  // namespace search_engine
