#pragma once
#include <memory>
#include <vector>
#include "core/ft/config/ftfuzzyconfig.h"
#include "core/ft/ft_fuzzy/advacedpackedvec.h"
#include "core/ft/ft_fuzzy/dataholder/basebuildedholder.h"
#include "core/ft/ftdsl.h"
#include "core/ft/idrelset.h"

namespace reindexer {
class RdxContext;
}  // namespace reindexer

namespace search_engine {

struct IDCtx {
	const reindexer::RVector<reindexer::IdRelType::PosType, 3>* data;
	int pos;
	double* max_proc;
	size_t total_size;
	const reindexer::FtDslOpts* opts;
	const reindexer::FtFuzzyConfig& cfg;
	double proc;
	word_size_map* sizes;
};
struct ResultMerger {
	int src_pos;
	int boost;
	int pos;
	double proc;
};

class MergedData {
public:
	MergedData(size_t id, const IDCtx& ctx) : id_(id) {
		size_it_ = ctx.sizes->find(id_);
		if (size_it_ == ctx.sizes->end()) {
			abort();
		}
		Add(ctx);
	}

	void Add(const IDCtx& ctx);
	double proc_ = 0;
	size_t id_;
	size_t count_ = 0;

private:
	bool first_ = true;
	ResultMerger prev_;
	word_size_map::iterator size_it_;
};
struct SearchResult {
	std::shared_ptr<std::vector<MergedData>> data_;
	double max_proc_;
};

struct FirstResult {
	const reindexer::AdvacedPackedVec* data;
	const reindexer::FtDslOpts* opts;
	int pos;
	double proc;
};

struct MergeCtx {
	std::vector<FirstResult>* results;
	const reindexer::FtFuzzyConfig* cfg;
	size_t total_size;
	word_size_map* sizes;
};

class BaseMerger {
public:
	BaseMerger(int max_id, int min_id);

	SearchResult Merge(MergeCtx& ctx, bool inTransaction, const reindexer::RdxContext&);

private:
	int max_id_;
	int min_id_;
};
}  // namespace search_engine
