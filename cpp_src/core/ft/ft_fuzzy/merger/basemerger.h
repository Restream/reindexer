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

struct [[nodiscard]] IDCtx {
	const reindexer::h_vector<reindexer::PosType, 3>* data;
	int pos;
	double* max_proc{nullptr};
	size_t total_size;
	const reindexer::FtDslOpts* opts{nullptr};
	const reindexer::FtFuzzyConfig& cfg;
	double proc;
	word_size_map* sizes{nullptr};
};
struct [[nodiscard]] ResultMerger {
	int src_pos;
	int boost;
	int pos;
	double proc;
};

class [[nodiscard]] MergedData {
public:
	MergedData(size_t id, const IDCtx& ctx) : id_(id) {
		size_it_ = ctx.sizes->find(id_);
		if (size_it_ == ctx.sizes->end()) {
			abort();
		}
		Add(ctx);
	}

	void Add(const IDCtx& ctx);
	double rank_ = 0;
	size_t id_;
	size_t count_ = 0;

private:
	bool first_ = true;
	ResultMerger prev_;
	word_size_map::iterator size_it_;
};
struct [[nodiscard]] SearchResult {
	std::shared_ptr<std::vector<MergedData>> data_;
	double max_proc_;
};

struct [[nodiscard]] FirstResult {
	const reindexer::AdvacedPackedVec* data{nullptr};
	const reindexer::FtDslOpts* opts{nullptr};
	int pos;
	double proc;
};

struct [[nodiscard]] MergeCtx {
	std::vector<FirstResult>* results{nullptr};
	const reindexer::FtFuzzyConfig* cfg{nullptr};
	size_t total_size;
	word_size_map* sizes{nullptr};
};

class [[nodiscard]] BaseMerger {
public:
	BaseMerger(int max_id, int min_id);

	SearchResult Merge(MergeCtx& ctx, bool inTransaction, const reindexer::RdxContext&) const;

private:
	int max_id_;
	int min_id_;
};
}  // namespace search_engine
