#pragma once
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ftdsl.h"
#include "core/ft/idrelset.h"
#include "core/idset.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "dataholder.h"

using std::vector;
namespace reindexer {

class Selecter {
public:
	Selecter(DataHolder& holder, size_t fieldSize, bool needArea) : holder_(holder), fieldSize_(fieldSize), needArea_(needArea) {}

	struct TextSearchResult {
		const PackedIdRelSet* vids_;
		const char* pattern;
		int proc_;
		int16_t wordLen_;
	};

	struct MergeInfo {
		IdType id;
		int proc;
		AreaHolder::UniquePtr holder;
		uint16_t step_id;
	};

	struct MergeData : public vector<MergeInfo> {
		int mergeCnt = 0;
	};

	struct MergedIdRel {
		IdRelType cur;
		IdRelType next;
		int rank;
		int qpos;
	};
	struct FtVariantEntry {
		string pattern;
		FtDslOpts opts;
		int proc;
	};
	class TextSearchResults : public h_vector<TextSearchResult, 8> {
	public:
		int idsCnt_ = 0;
		FtDSLEntry term;
	};

	MergeData Process(FtDSLQuery& dsl);
	struct FtSelectContext {
		vector<FtVariantEntry> variants;

		typename DataHolder::FondWordsType foundWords;
		vector<TextSearchResults> rawResults;
	};
	MergeData mergeResults(vector<TextSearchResults>& rawResults);
	void mergeItaration(TextSearchResults& rawRes, vector<bool>& exists, vector<MergeInfo>& merged, vector<MergedIdRel>& merged_rd,
						h_vector<int16_t>& idoffsets);

	void debugMergeStep(const char* msg, int vid, float normBm25, float normDist, int finalRank, int prevRank);
	void processVariants(FtSelectContext&);
	void prepareVariants(FtSelectContext&, FtDSLEntry&, std::vector<string>& langs);
	void processStepVariants(FtSelectContext& ctx, DataHolder::CommitStep& step, const FtVariantEntry& variant, TextSearchResults& res);

	void processTypos(FtSelectContext&, FtDSLEntry&);

	DataHolder& holder_;
	size_t fieldSize_;
	bool needArea_;
};

}  // namespace reindexer
