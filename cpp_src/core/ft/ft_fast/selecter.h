#pragma once
#include "core/ft/ftdsl.h"
#include "core/ft/idrelset.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "dataholder.h"

namespace reindexer {

template <typename IdCont>
class Selecter {
	using index_t = uint32_t;
	enum : index_t { kExcluded = std::numeric_limits<index_t>::max() };
	typedef fast_hash_map<WordIdType, pair<size_t, size_t>, WordIdTypeHash, WordIdTypequal> FondWordsType;

public:
	Selecter(DataHolder<IdCont>& holder, size_t fieldSize, bool needArea) : holder_(holder), fieldSize_(fieldSize), needArea_(needArea) {}

	struct TextSearchResult {
		const IdCont* vids_;
		std::string_view pattern;
		int proc_;
		int16_t wordLen_;
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

	IDataHolder::MergeData Process(FtDSLQuery& dsl, bool inTransaction, const RdxContext&);
	struct FtSelectContext {
		vector<FtVariantEntry> variants;

		FondWordsType foundWords;
		vector<TextSearchResults> rawResults;
	};
	IDataHolder::MergeData mergeResults(vector<TextSearchResults>& rawResults, const std::vector<size_t>& synonymsBounds,
										bool inTransaction, const RdxContext&);
	struct MergeStatus;
	void mergeItaration(const TextSearchResults& rawRes, index_t rawResIndex, std::vector<MergeStatus>& statuses,
						vector<IDataHolder::MergeInfo>& merged, vector<MergedIdRel>& merged_rd, vector<bool>& curExists, bool hasBeenAnd,
						bool simple, bool inTransaction, const RdxContext&);

	void debugMergeStep(const char* msg, int vid, float normBm25, float normDist, int finalRank, int prevRank);
	void processVariants(FtSelectContext&);
	void prepareVariants(std::vector<FtVariantEntry>&, size_t termIdx, const std::vector<string>& langs, const FtDSLQuery&,
						 std::vector<SynonymsDsl>*);
	void processStepVariants(FtSelectContext& ctx, typename DataHolder<IdCont>::CommitStep& step, const FtVariantEntry& variant,
							 TextSearchResults& res);

	void processTypos(FtSelectContext&, const FtDSLEntry&);

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
	bool needArea_;
};

extern template class Selecter<PackedIdRelVec>;
extern template class Selecter<IdRelVec>;

}  // namespace reindexer
