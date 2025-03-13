#pragma once
#include "core/enums.h"
#include "core/ft/areaholder.h"
#include "core/ft/ftdsl.h"
#include "core/ft/idrelset.h"
#include "core/index/ft_preselect.h"
#include "dataholder.h"

namespace reindexer {

// Final information about found document
struct MergeInfo {
	IdType id;	   // Virtual id of merged document (index in vdocs)
	int32_t proc;  // Rank of document
	uint32_t areaIndex = std::numeric_limits<uint32_t>::max();
	int8_t field;  // Field index, where was match
};

struct MergeDataBase : public std::vector<MergeInfo> {
	virtual ~MergeDataBase() = default;
	int maxRank = 0;
};

template <typename AreaType>
struct MergeData : public MergeDataBase {
	using AT = AreaType;
	std::vector<AreasInDocument<AreaType>> vectorAreas;
};

template <typename IdCont>
class Selector {
	typedef fast_hash_map<WordIdType, std::pair<size_t, size_t>, WordIdTypeHash, WordIdTypeEqual, WordIdTypeLess> FoundWordsType;

public:
	Selector(DataHolder<IdCont>& holder, size_t fieldSize, int maxAreasInDoc)
		: holder_(holder), fieldSize_(fieldSize), maxAreasInDoc_(maxAreasInDoc) {}

	// Intermediate information about document found at current merge step. Used only for queries with 2 or more terms
	struct MergedIdRel {
		explicit MergedIdRel(IdRelType&& c, int r, int q) : next(std::move(c)), rank(r), qpos(q) {}
		explicit MergedIdRel(int r, int q) : rank(r), qpos(q) {}
		MergedIdRel(MergedIdRel&&) noexcept = default;
		IdRelType cur;	 // Ids & pos of matched document of current step
		IdRelType next;	 // Ids & pos of matched document of next step
		int32_t rank;	 // Rank of current matched document
		int32_t qpos;	 // Position in query
	};

	struct MergedIdRelGroup : public MergedIdRel {
		explicit MergedIdRelGroup(IdRelType&& c, int r, int q) : MergedIdRel(r, q), posTmp(std::move(c)) {}
		MergedIdRelGroup(MergedIdRelGroup&&) noexcept = default;
		IdRelType posTmp;  // Group only. Collect all positions for subpatterns and index into vector we merged with
	};

	template <typename PosT>
	struct MergedIdRelGroupArea : public MergedIdRel {
		using TypeTParam = PosT;
		MergedIdRelGroupArea(IdRelType&& c, int r, int q, RVector<std::pair<PosT, int>, 4>&& p)
			: MergedIdRel(std::move(c), r, q), posTmp(std::move(p)) {}
		MergedIdRelGroupArea(MergedIdRelGroupArea&&) noexcept = default;

		RVector<std::pair<PosT, int>, 4>
			posTmp;	 // For group only. Collect all positions for subpatterns and the index in the vector with which we merged
		RVector<RVector<std::pair<PosT, int>, 4>, 2> wordPosForChain;
	};

	template <FtUseExternStatuses useExternSt, typename MergeType>
	MergeType Process(FtDSLQuery&& dsl, bool inTransaction, RankSortType, FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext&);

private:
	struct TextSearchResult {
		const IdCont* vids;		   // indexes of documents (vdoc) containing the given word + position + field
		std::string_view pattern;  // word,translit,.....
		int proc;
	};

	struct TermRankInfo {
		int32_t termRank = 0;
		double bm25Norm = 0.0;
		double termLenBoost = 0.0;
		double positionRank = 0.0;
		double normDist = 0.0;
		double proc = 0.0;
		double fullMatchBoost = 0.0;
		std::string_view pattern;
		std::string ftDslTerm;

		std::string ToString() const {
			return fmt::format(
				R"json({{term_rank:{}, term:{}, pattern:{}, bm25_norm:{}, term_len_boost:{}, position_rank:{}, norm_dist:{}, proc:{}, full_match_boost:{}}} )json",
				termRank, ftDslTerm, pattern, bm25Norm, termLenBoost, positionRank, normDist, proc, fullMatchBoost);
		}
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
		// Variants with low relevancy. For example, short terms, received from stemmers.
		// Those variants will be handled separately from main variants array (and some of them will probably be excluded)
		RVector<FtBoundVariantEntry, 4> lowRelVariants;

		// Found words map, shared between all the terms
		// The main purpose is to detect unique words and also reuse already allocated map buckets
		// This map is relevant only for sequential variants/terms handling
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
		void logTraceF(int level, fmt::format_string<Args...> fmt, Args&&... args);
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

	template <typename Bm25Type, typename MergedOffsetT, typename MergeType>
	MergeType mergeResults(std::vector<TextSearchResults>&& rawResults, size_t totalORVids, const std::vector<size_t>& synonymsBounds,
						   bool inTransaction, RankSortType, FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext&);

	template <typename Bm25Type, typename MergedOffsetT, typename MergeType>
	void mergeIteration(TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses, MergeType& merged,
						std::vector<MergedIdRel>& merged_rd, std::vector<MergedOffsetT>& idoffsets, std::vector<bool>& curExists,
						const bool hasBeenAnd, const bool inTransaction, const RdxContext&);

	template <typename PosType, typename Bm25Type, typename MergedOffsetT, typename MergeType>
	void mergeIterationGroup(TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses, MergeType& merged,
							 std::vector<PosType>& mergedPos, std::vector<MergedOffsetT>& idoffsets, std::vector<bool>& present,
							 const bool firstTerm, const bool inTransaction, const RdxContext& rdxCtx);

	template <typename PosType, typename Bm25T, typename MergedOffsetT, typename MergeType>
	void mergeGroupResult(std::vector<TextSearchResults>& rawResults, size_t from, size_t to, FtMergeStatuses::Statuses& mergeStatuses,
						  MergeType& merged, std::vector<MergedIdRel>& merged_rd, OpType op, const bool hasBeenAnd,
						  std::vector<MergedOffsetT>& idoffsets, const bool inTransaction, const RdxContext& rdxCtx);

	template <typename PosType, typename Bm25Type, typename MergedOffsetT, typename MergeType>
	void mergeResultsPart(std::vector<TextSearchResults>& rawResults, size_t from, size_t to, MergeType& merged,
						  std::vector<PosType>& mergedPos, const bool inTransaction, const RdxContext& rdxCtx);

	template <typename AreaType, typename PosT>
	AreasInDocument<AreaType> createAreaFromSubMerge(const MergedIdRelGroupArea<PosT>& posInfo);

	template <typename PosT>
	void insertSubMergeArea(const MergedIdRelGroupArea<PosT>& posInfo, PosT cur, int prevIndex, AreasInDocument<Area>& area);

	template <typename PosT>
	void insertSubMergeArea(const MergedIdRelGroupArea<PosT>& posInfo, PosT cur, int prevIndex, AreasInDocument<AreaDebug>& area);

	template <typename AreaType>
	void copyAreas(AreasInDocument<AreaType>& subMerged, AreasInDocument<AreaType>& merged, int32_t rank);

	template <typename PosType, typename MergedOffsetT, typename MergeType>
	void subMergeLoop(MergeType& subMerged, std::vector<PosType>& subMergedPos, MergeType& merged, std::vector<MergedIdRel>& merged_rd,
					  FtMergeStatuses::Statuses& mergeStatuses, std::vector<MergedOffsetT>& idoffsets, std::vector<bool>* checkAndOpMerge,
					  const bool hasBeenAnd);

	template <typename Calculator>
	void calcFieldBoost(const Calculator& bm25Calc, unsigned long long f, const IdRelType& relid, const FtDslOpts& opts,
						TermRankInfo& termInf, bool& dontSkipCurTermRank, h_vector<double, 4>& ranksInFields, int& field);

	template <typename Calculator>
	std::pair<double, int> calcTermRank(const TextSearchResults& rawRes, Calculator c, const IdRelType& relid, TermRankInfo& termInf);

	template <typename MergedOffsetT, typename MergeType>
	void addNewTerm(FtMergeStatuses::Statuses& mergeStatuses, MergeType& merged, std::vector<MergedOffsetT>& idoffsets,
					std::vector<bool>& curExists, const IdRelType& relid, index_t rawResIndex, int32_t termRank, int field);

	void addAreas(AreasInDocument<Area>& area, const IdRelType& relid, int32_t termRank, const TermRankInfo& termInf,
				  const std::wstring& pattern);
	void addAreas(AreasInDocument<AreaDebug>& area, const IdRelType& relid, int32_t termRank, const TermRankInfo& termInf,
				  const std::wstring& pattern);

	template <typename T, typename... Ts>
	constexpr static bool IsOneOf = (... || std::is_same_v<T, Ts>);

	template <typename PosType>
	static constexpr bool isSingleTermMerge() noexcept {
		static_assert(
			IsOneOf<PosType, MergedIdRelGroup, MergedIdRelGroupArea<IdRelType::PosType>, MergedIdRelGroupArea<PosTypeDebug>, MergedIdRel>,
			"unsupported type for mergeIteration");
		return std::is_same_v<PosType, MergedIdRel>;
	}

	template <typename PosType>
	static constexpr bool isGroupMerge() noexcept {
		static_assert(
			IsOneOf<PosType, MergedIdRelGroup, MergedIdRelGroupArea<IdRelType::PosType>, MergedIdRelGroupArea<PosTypeDebug>, MergedIdRel>,
			"unsupported type for mergeIteration");
		return std::is_same_v<PosType, MergedIdRelGroup>;
	}

	template <typename PosType>
	static constexpr bool isGroupMergeWithAreas() noexcept {
		static_assert(
			IsOneOf<PosType, MergedIdRelGroup, MergedIdRelGroupArea<IdRelType::PosType>, MergedIdRelGroupArea<PosTypeDebug>, MergedIdRel>,
			"unsupported type for mergeIteration");
		return std::is_same_v<PosType, MergedIdRelGroupArea<IdRelType::PosType>> ||
			   std::is_same_v<PosType, MergedIdRelGroupArea<PosTypeDebug>>;
	}

	template <typename MergedOffsetT, typename MergeType>
	MergeType mergeResultsBmType(std::vector<TextSearchResults>&& results, size_t totalORVids, const std::vector<size_t>& synonymsBounds,
								 bool inTransaction, RankSortType, FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext& rdxCtx);

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
	int maxAreasInDoc_;
};

}  // namespace reindexer
