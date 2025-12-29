#pragma once
#include "merger.h"

namespace reindexer {

template <typename IdCont>
class [[nodiscard]] Selector {
public:
	Selector(DataHolder<IdCont>& holder, size_t fieldSize, int maxAreasInDoc)
		: holder_(holder), fieldSize_(fieldSize), maxAreasInDoc_(maxAreasInDoc) {}

	template <FtUseExternStatuses useExternSt, typename MergedDataType>
	MergedDataType Process(FtDSLQuery&& query, bool inTransaction, RankSortType rankSortType, FtMergeStatuses::Statuses&& mergeStatuses,
						   const RdxContext&);

private:
	struct [[nodiscard]] FtVariantEntry {
		FtVariantEntry() = default;
		FtVariantEntry(std::string p, FtDslOpts o, float pr, int c) : pattern{std::move(p)}, opts{std::move(o)}, proc{pr}, charsCount{c} {}

		int GetLenCached() noexcept {
			if (charsCount < 0) {
				charsCount = getUTF8StringCharactersCount(pattern);
			}
			return charsCount;
		}

		std::string pattern;
		FtDslOpts opts;
		float proc;		 // Rank
		int charsCount;	 // UTF-8 characters count
	};

	struct [[nodiscard]] FtBoundVariantEntry : public FtVariantEntry {
		using FtVariantEntry::FtVariantEntry;

		int rawResultIdx = -1;
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
		size_t Process(const std::wstring& pattern, const std::vector<std::wstring>& variantsForTypos, const DataHolder<IdCont>& holder,
					   ft::TermResults<IdCont>& res, ft::FoundWordsProcsType& wordsProcs);

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
	void processLowRelVariants(size_t& totalORVids, h_vector<FtBoundVariantEntry, 4>& lowRelVariants,
							   const FtMergeStatuses::Statuses& mergeStatuses, std::vector<ft::TermResults<IdCont>>& rawResults,
							   std::deque<std::unique_ptr<ft::FoundWordsProcsType>>& termsWordsProcs);

	void applyStemmers(const std::string& pattern, int proc, const FtDslOpts& termOpts, bool keepSuffForStemmedVars,
					   std::vector<FtVariantEntry>& variants, h_vector<FtBoundVariantEntry, 4>* lowRelVariants, std::string& buffer);

	void prepareVariants(const FtDSLEntry& term, int baseRelevancy, unsigned termIdx, std::vector<FtVariantEntry>& variants,
						 h_vector<FtBoundVariantEntry, 4>* lowRelVariants, std::vector<MultiWord>* synonyms,
						 std::vector<std::wstring>* variantsForTypos);
	void prepareSynonymVariants(const std::wstring& pattern, const FtDslOpts& opts, std::vector<FtVariantEntry>& variants,
								std::string& patternBuf, std::string& stemmerBuf);

	template <FtUseExternStatuses>
	size_t processStepVariants(typename DataHolder<IdCont>::CommitStep& step, const FtVariantEntry& variant,
							   const FtMergeStatuses::Statuses& mergeStatuses, int vidsLimit, ft::TermResults<IdCont>& result,
							   ft::FoundWordsProcsType& wordsProcs);
	RX_NO_INLINE void printVariants(const std::vector<FtVariantEntry>& variants, const h_vector<FtBoundVariantEntry, 4>& lowRelVariants,
									const ft::TermResults<IdCont>& res);

	template <typename MergedOffsetT, typename MergedDataType>
	MergedDataType mergeResults(std::vector<ft::TermResults<IdCont>>& results, size_t totalORVids,
								const std::vector<size_t>& synonymsBounds, bool inTransaction, RankSortType rankSortType,
								FtMergeStatuses::Statuses& mergeStatuses, const RdxContext& rdxCtx);

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
	int maxAreasInDoc_;
};

}  // namespace reindexer
