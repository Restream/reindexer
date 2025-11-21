#pragma once
#include "merger.h"

namespace reindexer {

template <typename IdCont>
class [[nodiscard]] Selector {
public:
	Selector(DataHolder<IdCont>& holder, size_t fieldSize, int maxAreasInDoc)
		: holder_(holder), fieldSize_(fieldSize), maxAreasInDoc_(maxAreasInDoc) {}

	template <FtUseExternStatuses useExternSt, typename MergedDataType>
	MergedDataType Process(FtDSLQuery&& dsl, bool inTransaction, RankSortType rankSortType, FtMergeStatuses::Statuses&& mergeStatuses,
						   const RdxContext&);

private:
	struct [[nodiscard]] FtVariantEntry {
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

	struct [[nodiscard]] FtBoundVariantEntry : public FtVariantEntry {
		using FtVariantEntry::FtVariantEntry;

		int rawResultIdx = -1;
	};

	struct [[nodiscard]] FtSelectContext {
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
		h_vector<FtBoundVariantEntry, 4> lowRelVariants;

		// Found words map, shared between all the terms
		// The main purpose is to detect unique words and also reuse already allocated map buckets
		// This map is relevant only for sequential variants/terms handling
		FoundWordsType foundWordsSharedOR;
		// Optional words map for AND operations.
		// It will be cleared for each variant, so it's separated from OR/NOT words map
		std::unique_ptr<FoundWordsType> foundWordsSharedAND;
		std::vector<TextSearchResults<IdCont>> rawResults;
		std::vector<std::wstring> variantsForTypos;
		size_t totalORVids = 0;
	};

	class [[nodiscard]] TyposHandler {
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
		size_t Process(std::vector<TextSearchResults<IdCont>>&, const DataHolder<IdCont>&, const std::wstring& pattern,
					   const std::vector<std::wstring>& variantsForTypos);

	private:
		template <typename... Args>
		void logTraceF(int level, fmt::format_string<Args...> fmt, Args&&... args);
		bool isWordFitMaxTyposDist(const WordTypo& found, const typos_context::TyposVec& current);
		bool isWordFitMaxLettPerm(const std::string_view foundWord, const WordTypo& found, const std::wstring_view currentWord,
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

	template <FtUseExternStatuses>
	void processVariants(FtSelectContext&, const FtMergeStatuses::Statuses& mergeStatuses);
	template <FtUseExternStatuses>
	void processLowRelVariants(FtSelectContext&, const FtMergeStatuses::Statuses& mergeStatuses);

	void applyStemmers(const std::string& pattern, int proc, const std::vector<std::string>& langs, const FtDslOpts& termOpts,
					   bool keepSuffForStemmedVars, std::vector<FtVariantEntry>& variants, h_vector<FtBoundVariantEntry, 4>* lowRelVariants,
					   std::string& buffer);
	void prepareVariants(std::vector<FtVariantEntry>&, h_vector<FtBoundVariantEntry, 4>* lowRelVariants, size_t termIdx,
						 const std::vector<std::string>& langs, FtDSLQuery&, std::vector<SynonymsDsl>*,
						 std::vector<std::wstring>* variantsForTypos);
	template <FtUseExternStatuses>
	void processStepVariants(FtSelectContext& ctx, typename DataHolder<IdCont>::CommitStep& step, const FtVariantEntry& variant,
							 unsigned curRawResultIdx, const FtMergeStatuses::Statuses& mergeStatuses, int vidsLimit);
	RX_NO_INLINE void printVariants(const FtSelectContext& ctx, const TextSearchResults<IdCont>& res);

	template <typename MergedOffsetT, typename MergedDataType>
	MergedDataType mergeResults(std::vector<TextSearchResults<IdCont>>& results, size_t totalORVids,
								const std::vector<size_t>& synonymsBounds, bool inTransaction, RankSortType rankSortType,
								FtMergeStatuses::Statuses& mergeStatuses, const RdxContext& rdxCtx);

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
	int maxAreasInDoc_;
};

}  // namespace reindexer
