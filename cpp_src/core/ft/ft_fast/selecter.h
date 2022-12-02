#pragma once
#include "core/ft/ftdsl.h"
#include "core/ft/idrelset.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "dataholder.h"

namespace reindexer {

template <typename IdCont>
class Selecter {
	typedef fast_hash_map<WordIdType, std::pair<size_t, size_t>, WordIdTypeHash, WordIdTypequal> FondWordsType;

public:
	Selecter(DataHolder<IdCont>& holder, size_t fieldSize, bool needArea, int maxAreasInDoc)
		: holder_(holder), fieldSize_(fieldSize), needArea_(needArea), maxAreasInDoc_(maxAreasInDoc) {}

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
		std::string pattern;
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

	template <bool mergeStatusesEmpty>
	IDataHolder::MergeData Process(FtDSLQuery& dsl, bool inTransaction, FtMergeStatuses::Statuses mergeStatuses, const RdxContext&);
	struct FtSelectContext {
		std::vector<FtVariantEntry> variants;

		FondWordsType foundWords;
		std::vector<TextSearchResults> rawResults;
	};
	IDataHolder::MergeData mergeResults(std::vector<TextSearchResults>& rawResults, const std::vector<size_t>& synonymsBounds,
										bool inTransaction, FtMergeStatuses::Statuses mergeStatuses, const RdxContext&);
	void mergeItaration(const TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses,
						std::vector<IDataHolder::MergeInfo>& merged, std::vector<MergedIdRel>& merged_rd, std::vector<uint16_t>& idoffsets,
						std::vector<bool>& curExists, bool hasBeenAnd, bool inTransaction, const RdxContext&);

	void debugMergeStep(const char* msg, int vid, float normBm25, float normDist, int finalRank, int prevRank);
	template <bool withStatuses>
	void processVariants(FtSelectContext&, const FtMergeStatuses::Statuses& mergeStatuses);
	void prepareVariants(std::vector<FtVariantEntry>&, size_t termIdx, const std::vector<std::string>& langs, const FtDSLQuery&,
						 std::vector<SynonymsDsl>*);
	template <bool withStatuses>
	void processStepVariants(FtSelectContext& ctx, typename DataHolder<IdCont>::CommitStep& step, const FtVariantEntry& variant,
							 TextSearchResults& res, const FtMergeStatuses::Statuses& mergeStatuses);

	void processTypos(FtSelectContext&, const FtDSLEntry&);

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
	bool needArea_;
	int maxAreasInDoc_;
};

extern template class Selecter<PackedIdRelVec>;
extern template class Selecter<IdRelVec>;

}  // namespace reindexer
