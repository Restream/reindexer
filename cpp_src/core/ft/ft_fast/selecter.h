#pragma once
#include "merger.h"

namespace reindexer {

// Minimal relevant length of the stemmer's term
constexpr int kMinStemRelevantLen = 3;
// Max length of the stemming result, which will be skipped
constexpr int kMaxStemSkipLen = 1;

constexpr int kMinTypoVariantStemLen = 5;
constexpr int kMinSplitVariantStemLen = 5;

class [[nodiscard]] TermVariant {
public:
	TermVariant() = default;
	TermVariant(const std::wstring& p, float pr, const FtDslOpts& opts)
		: pattern{p}, proc{pr}, pref{opts.pref}, suff{opts.suff}, typos{opts.typos} {}
	TermVariant(std::wstring&& p, float pr, const TermVariant& other)
		: pattern{std::move(p)},
		  proc{pr},
		  stem{other.stem},
		  synonyms{other.synonyms},
		  pref{other.pref},
		  suff{other.suff},
		  typos{other.typos},
		  lowRelevance{other.lowRelevance} {}
	TermVariant(std::string_view p, float pr, const TermVariant& other)
		: proc{pr},
		  stem{other.stem},
		  synonyms{other.synonyms},
		  pref{other.pref},
		  suff{other.suff},
		  typos{other.typos},
		  lowRelevance{other.lowRelevance},
		  patternUtf8{p} {
		utf8_to_utf16(patternUtf8, pattern);
	}

	TermVariant(TermVariant&& other) = default;
	TermVariant& operator=(TermVariant&& other) = default;

	void Unite(const TermVariant& other, float pr) {
		proc = std::max(proc, pr);
		stem |= other.stem;
		pref |= other.pref;
		suff |= other.suff;
		typos |= other.typos;
	}

	std::wstring pattern;
	float proc = 0.0f;
	float boost = -1.0f;

	bool stem = true;
	bool synonyms = true;
	bool pref = false;
	bool suff = false;
	bool typos = false;
	bool lowRelevance = false;

	const std::string& PatternUtf8() {
		if (patternUtf8.empty() && !pattern.empty()) {
			utf16_to_utf8(pattern, patternUtf8);
		}

		return patternUtf8;
	}

	std::string FullPattern() { return (suff ? "*" : "") + PatternUtf8() + (pref ? "*" : ""); }

private:
	std::string patternUtf8;
};

class [[nodiscard]] TermVariants {
public:
	using StorageType = h_vector<TermVariant, 5>;

	TermVariants() = default;
	TermVariants(const TermVariants&) = default;
	TermVariants(TermVariants&&) noexcept = default;
	TermVariants(const FtDslOpts& opts) : termOpts_(opts) {}

	void SortByProc() {
		boost::sort::pdqsort_branchless(termVariants_.begin(), termVariants_.end(),
										[](const TermVariant& l, const TermVariant& r) noexcept { return l.proc > r.proc; });
	}

	StorageType::iterator begin() noexcept { return termVariants_.begin(); }
	StorageType::iterator end() noexcept { return termVariants_.end(); }
	StorageType::const_iterator begin() const noexcept { return termVariants_.begin(); }
	StorageType::const_iterator end() const noexcept { return termVariants_.end(); }

	void emplace_back(const std::wstring& p, float pr) { termVariants_.emplace_back(p, pr, termOpts_); }
	void emplace_back(std::wstring&& p, float pr) { termVariants_.emplace_back(std::move(p), pr, termOpts_); }
	void emplace_back(std::wstring&& p, float pr, const FtDslOpts& opts) { termVariants_.emplace_back(std::move(p), pr, opts); }
	void emplace_back(TermVariant&& v) { termVariants_.emplace_back(std::move(v)); }

	void reserve(size_t capacity) { termVariants_.reserve(capacity); }
	size_t size() const noexcept { return termVariants_.size(); }

	TermVariant& operator[](size_t idx) noexcept { return termVariants_[idx]; }
	const TermVariant& operator[](size_t idx) const noexcept { return termVariants_[idx]; }

	OpType Op() const noexcept { return termOpts_.op; }

private:
	StorageType termVariants_;
	FtDslOpts termOpts_;
};

template <typename IdCont>
class [[nodiscard]] Selector {
public:
	Selector(DataHolder<IdCont>& holder, size_t fieldSize, int maxAreasInDoc)
		: holder_(holder), fieldSize_(fieldSize), maxAreasInDoc_(maxAreasInDoc) {}

	template <FtUseExternStatuses useExternSt, typename MergedDataType>
	MergedDataType Process(FtDSLQuery&& query, bool inTransaction, RankSortType rankSortType, FtMergeStatuses::Statuses&& docsExcluded,
						   const RdxContext&);

private:
	float getTermBoost(const std::string& term) {
		auto it = holder_.stemmedTermsBoost.find(term);
		return it == holder_.stemmedTermsBoost.end() ? -1.0f : it->second;
	}

	void filterStopWordsAndAdd(TermVariants& termVariants, h_vector<TermVariant, 5>& newVariants) const;

	void tryToCorrectKbLayout(TermVariants& termVariants);
	void tryToSplit(TermVariants& termVariants, PhraseTerm phraseTerm);
	void tryToCorrectTypos(TermVariants& termVariants);
	void transliterate(TermVariants& termVariants);
	void stem(TermVariants& termVariants);
	void addSynonyms(TermVariants& termVariants);
	void boostVariants(TermVariants& termVariants);

	template <FtUseExternStatuses useExternSt>
	ft::TermResults<IdCont> buildTermResults(const FtDSLEntry& term, TermVariants& termVariants,
											 const FtMergeStatuses::Statuses& docsExcluded);

	template <FtUseExternStatuses useExternSt>
	void buildQueryMergeData(FtDSLQuery&& query, const FtMergeStatuses::Statuses& docsExcluded, bool inTransaction,
							 const RdxContext& rdxCtx, ft::QueryMergeData<IdCont>& queryMergeData);

	template <typename MergedOffsetT, typename MergedDataType>
	MergedDataType mergeResults(ft::QueryMergeData<IdCont>& queryMergeData, RankSortType rankSortType,
								FtMergeStatuses::Statuses& docsExcluded, bool inTransaction, const RdxContext& rdxCtx);

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
	int maxAreasInDoc_;

	h_vector<TermVariant, 5> newVariants;
};

}  // namespace reindexer
