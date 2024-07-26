#pragma once
#include "core/ft/ftdsl.h"
#include "core/ft/idrelset.h"
#include "dataholder.h"

namespace reindexer {

struct MergeInfo {
	IdType id;	   // Virtual id of merged document (index in vdocs)
	int32_t proc;  // Rank of document
	uint32_t areaIndex = std::numeric_limits<uint32_t>::max();
	int8_t field;  // Field index, where was match
};

struct MergeData : public std::vector<MergeInfo> {
	int maxRank = 0;
	std::vector<AreaHolder> vectorAreas;
};

template <typename IdCont>
class Selecter {
	typedef fast_hash_map<WordIdType, std::pair<size_t, size_t>, WordIdTypeHash, WordIdTypeEqual, WordIdTypeLess> FoundWordsType;

public:
	Selecter(DataHolder<IdCont>& holder, size_t fieldSize, bool needArea, int maxAreasInDoc)
		: holder_(holder), fieldSize_(fieldSize), needArea_(needArea), maxAreasInDoc_(maxAreasInDoc) {}

	// Intermediate information about found document in current merge step. Used only for queries with 2 or more terms
	struct MergedIdRel {
		explicit MergedIdRel(IdRelType&& c, int r, int q) : next(std::move(c)), rank(r), qpos(q) {}
		explicit MergedIdRel(int r, int q) : rank(r), qpos(q) {}
		MergedIdRel(MergedIdRel&&) = default;
		IdRelType cur;	 // Ids & pos of matched document of current step
		IdRelType next;	 // Ids & pos of matched document of next step
		int32_t rank;	 // Rank of curent matched document
		int32_t qpos;	 // Position in query
	};

	struct MergedIdRelEx : public MergedIdRel {
		explicit MergedIdRelEx(IdRelType&& c, int r, int q) : MergedIdRel(r, q), posTmp(std::move(c)) {}
		MergedIdRelEx(MergedIdRelEx&&) = default;
		IdRelType posTmp;  // For group only. Collect all positions for subpatterns and the index in the vector with which we merged
	};

	struct MergedIdRelExArea : public MergedIdRel {
		MergedIdRelExArea(IdRelType&& c, int r, int q, RVector<std::pair<IdRelType::PosType, int>, 4>&& p)
			: MergedIdRel(std::move(c), r, q), posTmp(std::move(p)) {}
		MergedIdRelExArea(MergedIdRelExArea&&) = default;

		RVector<std::pair<IdRelType::PosType, int>, 4>
			posTmp;	 // For group only. Collect all positions for subpatterns and the index in the vector with which we merged
		h_vector<RVector<std::pair<IdRelType::PosType, int>, 4>, 2> wordPosForChain;
	};

	template <FtUseExternStatuses>
	MergeData Process(FtDSLQuery&& dsl, bool inTransaction, FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext&);

private:
	struct TextSearchResult {
		const IdCont* vids;		   // indexes of documents (vdoc) containing the given word + position + field
		std::string_view pattern;  // word,translit,.....
		int proc;
		int16_t wordLen;
	};

	struct FtVariantEntry {
		FtVariantEntry() = default;
		FtVariantEntry(std::string p, FtDslOpts o, int pr, int c) : pattern{std::move(p)}, opts{std::move(o)}, proc{pr}, charsCount{c} {}

		int GetLenCached() noexcept {
			if (charsCount < 0) {
				charsCount = getUTF8StringCharactersCount(pattern);
			}
			return charsCount;
		}

		std::string pattern;
		FtDslOpts opts;
		int proc;		 // Rank
		int charsCount;	 // UTF-8 characters count
	};

	// text search results for a single token (word) in a search query
	class TextSearchResults : public RVector<TextSearchResult, 8> {
	public:
		TextSearchResults(FtDSLEntry&& t, FoundWordsType* fwPtr) : term(std::move(t)), foundWords(fwPtr) { assertrx(foundWords); }
		void SwitchToInternalWordsMap() noexcept {
			if (!foundWordsPersonal_) {
				foundWordsPersonal_ = std::make_unique<FoundWordsType>();
			}
			foundWords = foundWordsPersonal_.get();
		}

		uint32_t idsCnt_ = 0;
		FtDSLEntry term;
		std::vector<size_t> synonyms;
		std::vector<size_t> synonymsGroups;
		FoundWordsType* foundWords = nullptr;

	private:
		// Internal words map.
		// This map will be used instead of shared version for terms with 'irrelevant' variants
		std::unique_ptr<FoundWordsType> foundWordsPersonal_;
	};

	struct FtBoundVariantEntry : public FtVariantEntry {
		using FtVariantEntry::FtVariantEntry;

		int rawResultIdx = -1;
	};

	struct FtSelectContext {
		FoundWordsType* GetWordsMapPtr(const FtDslOpts& opts) {
			if (opts.op == OpAnd) {
				if (!foundWordsSharedAND) {
					foundWordsSharedAND = std::make_unique<FoundWordsType>();
				}
				return foundWordsSharedAND.get();
			}
			return &foundWordsSharedOR;
		}

		std::vector<FtVariantEntry> variants;
		// Variants with low relevancy. For example, short terms, recieved from stemmers.
		// Those variants will be handled separately from main variants array (and some of them will probably be excluded)
		RVector<FtBoundVariantEntry, 4> lowRelVariants;

		// Found words map, shared between all the terms
		// The main purpose is to detect unique words and also reuse already allocated map buckets
		// This map is rellevant only for sequential varinants/terms handling
		FoundWordsType foundWordsSharedOR;
		// Optional words map for AND operations.
		// It will be cleared for each variant, so it's separated from OR/NOT words map
		std::unique_ptr<FoundWordsType> foundWordsSharedAND;
		std::vector<TextSearchResults> rawResults;
		size_t totalORVids = 0;
	};

	class TyposHandler {
	public:
		TyposHandler(const FtFastConfig& cfg) noexcept
			: maxTyposInWord_(cfg.MaxTyposInWord()),
			  dontUseMaxTyposForBoth_(maxTyposInWord_ != cfg.maxTypos / 2),
			  maxMissingLetts_(cfg.MaxMissingLetters()),
			  maxExtraLetts_(cfg.MaxExtraLetters()),
			  logLevel_(cfg.logLevel) {
			{
				const auto maxTypoDist = cfg.MaxTypoDistance();
				maxTypoDist_ = maxTypoDist.first;
				useMaxTypoDist_ = maxTypoDist.second;
			}
			{
				const auto maxLettPermDist = cfg.MaxSymbolPermutationDistance();
				maxLettPermDist_ = maxLettPermDist.first;
				useMaxLettPermDist_ = maxLettPermDist.second;
			}
		}
		size_t Process(std::vector<TextSearchResults>&, const DataHolder<IdCont>&, const FtDSLEntry&);

	private:
		template <typename... Args>
		void logTraceF(int level, const char* fmt, Args&&... args);
		bool isWordFitMaxTyposDist(const WordTypo& found, const typos_context::TyposVec& current);
		bool isWordFitMaxLettPerm(const std::string_view foundWord, const WordTypo& found, const std::wstring& currentWord,
								  const typos_context::TyposVec& current);

		const int maxTyposInWord_;
		const bool dontUseMaxTyposForBoth_;
		bool useMaxTypoDist_;
		bool useMaxLettPermDist_;
		unsigned maxTypoDist_;
		unsigned maxLettPermDist_;
		unsigned maxMissingLetts_;
		unsigned maxExtraLetts_;
		int logLevel_;
		std::wstring foundWordUTF16_;
	};

	template <typename Bm25Type, typename MergedOffsetT>
	MergeData mergeResults(std::vector<TextSearchResults>&& rawResults, size_t maxMergedSize, const std::vector<size_t>& synonymsBounds,
						   bool inTransaction, FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext&);

	template <typename Bm25Type, typename MergedOffsetT>
	void mergeIteration(TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses, MergeData& merged,
						std::vector<MergedIdRel>& merged_rd, std::vector<MergedOffsetT>& idoffsets, std::vector<bool>& curExists,
						const bool hasBeenAnd, const bool inTransaction, const RdxContext&);

	template <typename P, typename Bm25Type, typename MergedOffsetT>
	void mergeIterationGroup(TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses, MergeData& merged,
							 std::vector<P>& merged_rd, std::vector<MergedOffsetT>& idoffsets, std::vector<bool>& present,
							 const bool firstTerm, const bool inTransaction, const RdxContext& rdxCtx);

	template <typename PosType, typename Bm25T, typename MergedOffsetT>
	void mergeGroupResult(std::vector<TextSearchResults>& rawResults, size_t from, size_t to, FtMergeStatuses::Statuses& mergeStatuses,
						  MergeData& merged, std::vector<MergedIdRel>& merged_rd, OpType op, const bool hasBeenAnd,
						  std::vector<MergedOffsetT>& idoffsets, const bool inTransaction, const RdxContext& rdxCtx);

	template <typename PosType, typename Bm25Type, typename MergedOffsetT>
	void mergeResultsPart(std::vector<TextSearchResults>& rawResults, size_t from, size_t to, MergeData& merged,
						  std::vector<PosType>& mergedPos, const bool inTransaction, const RdxContext& rdxCtx);
	AreaHolder createAreaFromSubMerge(const MergedIdRelExArea& posInfo);
	void copyAreas(AreaHolder& subMerged, AreaHolder& merged, int32_t rank);

	template <typename PosType, typename MergedOffsetT>
	void subMergeLoop(std::vector<MergeInfo>& subMerged, std::vector<PosType>& subMergedPos, MergeData& merged,
					  std::vector<MergedIdRel>& merged_rd, FtMergeStatuses::Statuses& mergeStatuses, std::vector<MergedOffsetT>& idoffsets,
					  std::vector<bool>* checkAndOpMerge, const bool hasBeenAnd);

	template <typename Calculator>
	void calcFieldBoost(const Calculator& bm25Calc, unsigned long long f, const IdRelType& relid, const FtDslOpts& opts, int termProc,
						double& termRank, double& normBm25, bool& dontSkipCurTermRank, h_vector<double, 4>& ranksInFields, int& field);
	template <typename Calculator>
	std::pair<double, int> calcTermRank(const TextSearchResults& rawRes, Calculator c, const IdRelType& relid, int proc);

	template <typename MergedOffsetT>
	void addNewTerm(FtMergeStatuses::Statuses& mergeStatuses, MergeData& merged, std::vector<MergedOffsetT>& idoffsets,
					std::vector<bool>& curExists, const IdRelType& relid, index_t rawResIndex, int32_t termRank, int field);

	void addAreas(MergeData& merged, int32_t areaIndex, const IdRelType& relid, int32_t termRank);

	template <typename PosType>
	static constexpr bool isSingleTermMerge() noexcept {
		static_assert(
			std::is_same_v<PosType, MergedIdRelEx> || std::is_same_v<PosType, MergedIdRelExArea> || std::is_same_v<PosType, MergedIdRel>,
			"unsupported type for mergeIteration");
		return std::is_same_v<PosType, MergedIdRel>;
	}

	template <typename PosType>
	static constexpr bool isGroupMerge() noexcept {
		static_assert(
			std::is_same_v<PosType, MergedIdRelEx> || std::is_same_v<PosType, MergedIdRelExArea> || std::is_same_v<PosType, MergedIdRel>,
			"unsupported type for mergeIteration");
		return std::is_same_v<PosType, MergedIdRelEx>;
	}

	template <typename PosType>
	static constexpr bool isGroupMergeWithAreas() noexcept {
		static_assert(
			std::is_same_v<PosType, MergedIdRelEx> || std::is_same_v<PosType, MergedIdRelExArea> || std::is_same_v<PosType, MergedIdRel>,
			"unsupported type for mergeIteration");
		return std::is_same_v<PosType, MergedIdRelExArea>;
	}

	template <typename MergedOffsetT>
	MergeData mergeResultsBmType(std::vector<TextSearchResults>&& results, size_t totalORVids, const std::vector<size_t>& synonymsBounds,
								 bool inTransaction, FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext& rdxCtx);

	void debugMergeStep(const char* msg, int vid, float normBm25, float normDist, int finalRank, int prevRank);
	template <FtUseExternStatuses>
	void processVariants(FtSelectContext&, const FtMergeStatuses::Statuses& mergeStatuses);
	template <FtUseExternStatuses>
	void processLowRelVariants(FtSelectContext&, const FtMergeStatuses::Statuses& mergeStatuses);
	void prepareVariants(std::vector<FtVariantEntry>&, RVector<FtBoundVariantEntry, 4>* lowRelVariants, size_t termIdx,
						 const std::vector<std::string>& langs, const FtDSLQuery&, std::vector<SynonymsDsl>*);
	template <FtUseExternStatuses>
	void processStepVariants(FtSelectContext& ctx, typename DataHolder<IdCont>::CommitStep& step, const FtVariantEntry& variant,
							 unsigned curRawResultIdx, const FtMergeStatuses::Statuses& mergeStatuses, int vidsLimit);
	RX_NO_INLINE void printVariants(const FtSelectContext& ctx, const TextSearchResults& res);

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
	const bool needArea_;
	int maxAreasInDoc_;
};

extern template class Selecter<PackedIdRelVec>;
extern template class Selecter<IdRelVec>;

}  // namespace reindexer
