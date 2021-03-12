#pragma once
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ftdsl.h"
#include "core/ft/idrelset.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "dataholder.h"

namespace reindexer {

class Selecter {
	using index_t = uint32_t;
	enum : index_t { kExcluded = std::numeric_limits<index_t>::max() };

public:
	Selecter(DataHolder& holder, size_t fieldSize, bool needArea) : holder_(holder), fieldSize_(fieldSize), needArea_(needArea) {}

	struct TextSearchResult {
		const PackedIdRelSet* vids_;
		string_view pattern;
		int proc_;
		int16_t wordLen_;
	};

	// Final information about found document
	struct MergeInfo {
		IdType id;		 // Virual of merged document (index in vdocs)
		int16_t proc;	 // Rank of document
		int8_t matched;	 // Count of matched terms in document
		int8_t field;	 // Field index, where was match
		AreaHolder::UniquePtr holder;
	};

	struct MergeData : public vector<MergeInfo> {
		int mergeCnt = 0;
	};

	// Intermediate information about found document in current merge step. Used only for queries with 2 or more terms
	struct MergedIdRel {
		IdRelType cur;	 // Ids & pos of matched document of current step
		IdRelType next;	 // Ids & pos of matched document of next step
		int rank;		 // Rank of curent matched document
		int qpos;		 // Position in query
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
		std::vector<size_t> synonyms;
		std::vector<size_t> synonymsGroups;
	};

	MergeData Process(FtDSLQuery& dsl);
	struct FtSelectContext {
		vector<FtVariantEntry> variants;

		typename DataHolder::FondWordsType foundWords;
		vector<TextSearchResults> rawResults;
	};
	MergeData mergeResults(vector<TextSearchResults>& rawResults, const std::vector<size_t>& synonymsBounds);
	void mergeItaration(const TextSearchResults& rawRes, index_t rawResIndex, fast_hash_map<VDocIdType, index_t>& added,
						vector<MergeInfo>& merged, vector<MergedIdRel>& merged_rd, h_vector<int16_t>& idoffsets, vector<bool>& curExists,
						bool hasBeenAnd);

	void debugMergeStep(const char* msg, int vid, float normBm25, float normDist, int finalRank, int prevRank);
	void processVariants(FtSelectContext&);
	void prepareVariants(std::vector<FtVariantEntry>&, size_t termIdx, const std::vector<string>& langs, const FtDSLQuery&,
						 std::vector<SynonymsDsl>*);
	void processStepVariants(FtSelectContext& ctx, DataHolder::CommitStep& step, const FtVariantEntry& variant, TextSearchResults& res);

	void processTypos(FtSelectContext&, const FtDSLEntry&);

	DataHolder& holder_;
	size_t fieldSize_;
	bool needArea_;
};

}  // namespace reindexer
