#pragma once
#include <algorithm>
#include <memory>
#include <variant>
#include "core/id_type.h"
#include "estl/dynamic_bitset.h"
#include "indextexttypes.h"

namespace reindexer {

namespace ft {

using BitsetType = DynamicBitset<64>;

template <typename IdCont>
class [[nodiscard]] SubtermResults {
public:
	struct [[nodiscard]] HoldT {};
	struct [[nodiscard]] NoHoldT {};

	SubtermResults(std::shared_ptr<const IdCont> vids, std::string&& pattern, WordIdType patternId, float proc, float termLenBoost,
				   HoldT) noexcept
		: proc_(proc), termLenBoost_(termLenBoost), vids_(std::move(vids)), pattern_(std::move(pattern)), patternId_(patternId) {}
	SubtermResults(std::shared_ptr<const IdCont> vids, std::string_view pattern, WordIdType patternId, float proc, float termLenBoost,
				   NoHoldT) noexcept
		: proc_(proc), termLenBoost_(termLenBoost), vids_(std::move(vids)), pattern_(std::move(pattern)), patternId_(patternId) {}

	const IdCont& Occurences() const noexcept { return *vids_; }
	// NOLINTNEXTLINE(bugprone-exception-escape)
	std::string_view Pattern() const noexcept {
		return std::visit([](const auto& v) { return std::string_view(v); }, pattern_);
	}
	WordIdType PatternID() const noexcept { return patternId_; }
	float Proc() const noexcept { return proc_; }
	float TermLenBoost() const noexcept { return termLenBoost_; }
	void UpdateScore(float proc) noexcept { proc_ = std::max(proc_, proc); }
	bool Suppressed() const noexcept { return suppressed_; }
	void SetSuppressed(bool value = true) noexcept { suppressed_ = value; }

private:
	float proc_ = 0.0;
	float termLenBoost_ = 1.0f;
	bool suppressed_ = false;

	std::shared_ptr<const IdCont> vids_;				   // snapshot of postings for the given word + position + field
	std::variant<std::string, std::string_view> pattern_;  // word,translit,.....
	WordIdType patternId_;
};

// text search results for a single token (word) in a search query
template <typename IdCont>
class [[nodiscard]] TermResults {
public:
	TermResults() = default;
	TermResults(TermResults&&) = default;
	TermResults(const FtDslTerm& t) : term_(t) {}
	TermResults(FtDslTerm&& t) : term_(std::move(t)) {}

	const OpType& Op() const noexcept { return term_.Opts().op; }
	const std::u16string& Pattern() const noexcept { return term_.Pattern(); }
	const FtDslOpts& Opts() const noexcept { return term_.Opts(); }

	void AddSubterm(std::shared_ptr<const IdCont> vids, std::string_view pattern, WordIdType patternId, float proc, float termLenBoost) {
		subtermsResults_.emplace_back(std::move(vids), pattern, patternId, proc, termLenBoost,
									  typename ft::SubtermResults<IdCont>::NoHoldT{});
		maxVDocs_ += subtermsResults_.back().Occurences().size();
	}
	void AddSubterm(std::shared_ptr<const IdCont> vids, std::string&& pattern, WordIdType patternId, float proc, float termLenBoost) {
		subtermsResults_.emplace_back(std::move(vids), std::move(pattern), patternId, proc, termLenBoost,
									  typename ft::SubtermResults<IdCont>::HoldT{});
		maxVDocs_ += subtermsResults_.back().Occurences().size();
	}

	void SortSubterms() {
		boost::sort::pdqsort_branchless(
			subtermsResults_.begin(), subtermsResults_.end(),
			[](const SubtermResults<IdCont>& l, const SubtermResults<IdCont>& r) noexcept { return l.Proc() > r.Proc(); });
	}

	size_t EstimateNumDocsInMerge() const noexcept {
		size_t estimatedNumDocs = 0;
		for (const SubtermResults<IdCont>& subterm : subtermsResults_) {
			estimatedNumDocs += subterm.Occurences().size();
		}

		return estimatedNumDocs;
	}

	const h_vector<uint32_t, 2>& Synonyms() const noexcept { return synonyms_; }

	h_vector<SubtermResults<IdCont>, 8>::iterator begin() noexcept { return subtermsResults_.begin(); }
	h_vector<SubtermResults<IdCont>, 8>::iterator end() noexcept { return subtermsResults_.end(); }
	h_vector<SubtermResults<IdCont>, 8>::const_iterator begin() const noexcept { return subtermsResults_.begin(); }
	h_vector<SubtermResults<IdCont>, 8>::const_iterator end() const noexcept { return subtermsResults_.end(); }
	size_t NumSubterms() const noexcept { return subtermsResults_.size(); }

	SubtermResults<IdCont>& Subterm(size_t idx) noexcept { return subtermsResults_[idx]; }
	const SubtermResults<IdCont>& Subterm(size_t idx) const noexcept { return subtermsResults_[idx]; }

	uint32_t MaxVDocs() const noexcept { return maxVDocs_; }

private:
	uint32_t maxVDocs_ = 0;
	FtDslTerm term_;

	h_vector<SubtermResults<IdCont>, 8> subtermsResults_;
	h_vector<uint32_t, 2> synonyms_;
};

template <typename IdCont>
class [[nodiscard]] PhraseResults {
public:
	PhraseResults() = default;
	PhraseResults(const FtDslPhrase& phrase) : op_{phrase.Op()}, fieldsOpts_{phrase.FieldsOpts()}, distance_{phrase.Distance()} {}
	PhraseResults(PhraseResults&&) = default;
	PhraseResults& operator=(PhraseResults&&) = default;

	size_t NumTerms() const noexcept { return terms_.size(); }

	TermResults<IdCont>& Term(size_t idx) noexcept { return terms_[idx]; }

	void SortSubterms() {
		for (auto& t : terms_) {
			t.SortSubterms();
		}
	}

	const OpType& Op() const noexcept { return op_; }
	const h_vector<FtDslFieldOpts, 8>& FieldsOpts() const noexcept { return fieldsOpts_; }
	unsigned Distance() const noexcept { return distance_; }

	uint16_t CalcProc16() {
		long long sumProc = 0;
		for (auto& term : terms_) {
			if (term.NumSubterms() > 0) {
				sumProc += term.Subterm(0).Proc();
			}
		}

		assertrx_throw(sumProc >= 0 && sumProc < std::numeric_limits<uint16_t>::max());
		return static_cast<uint16_t>(sumProc);
	}

	void clear() { terms_.resize(0); }
	void Add(TermResults<IdCont>&& tr) { terms_.emplace_back(std::move(tr)); }

private:
	h_vector<TermResults<IdCont>, 3> terms_;
	OpType op_ = OpOr;
	h_vector<FtDslFieldOpts, 8> fieldsOpts_;
	unsigned distance_ = 1;
};

template <typename IdCont>
class [[nodiscard]] PhraseOrTerm {
public:
	PhraseOrTerm(TermResults<IdCont>&& tr) : data_(std::move(tr)) {}
	PhraseOrTerm(PhraseResults<IdCont>&& pr) : data_(std::move(pr)) {}

	bool IsTerm() const noexcept { return data_.index() == 0; }
	bool IsPhrase() const noexcept { return data_.index() == 1; }

	PhraseResults<IdCont>& Phrase() { return std::get<PhraseResults<IdCont>>(data_); }
	const PhraseResults<IdCont>& Phrase() const { return std::get<PhraseResults<IdCont>>(data_); }

	TermResults<IdCont>& Term() { return std::get<TermResults<IdCont>>(data_); }
	const TermResults<IdCont>& Term() const { return std::get<TermResults<IdCont>>(data_); }

	void SortSubterms() {
		std::visit([](auto& pot) { pot.SortSubterms(); }, data_);
	}

	OpType Op() const {
		return std::visit([](auto& pot) { return pot.Op(); }, data_);
	}

	const h_vector<size_t, 1>& SynonymsIds() const noexcept { return synonymsIds_; }
	void AddSynonymId(size_t id) { synonymsIds_.emplace_back(id); }

private:
	std::variant<TermResults<IdCont>, PhraseResults<IdCont>> data_;
	h_vector<size_t, 1> synonymsIds_;
};

template <typename IdCont>
class [[nodiscard]] Synonym {
public:
	size_t NumTerms() const noexcept { return terms_.size(); }

	TermResults<IdCont>& Term(size_t idx) noexcept { return terms_[idx]; }

	const std::vector<TermResults<IdCont>>& Terms() const noexcept { return terms_; }
	std::vector<TermResults<IdCont>>& Terms() noexcept { return terms_; }

	void AddTerm(TermResults<IdCont>&& term) { terms_.emplace_back(std::move(term)); }

	std::vector<TermResults<IdCont>>::iterator begin() noexcept { return terms_.begin(); }
	std::vector<TermResults<IdCont>>::iterator end() noexcept { return terms_.end(); }
	std::vector<TermResults<IdCont>>::const_iterator begin() const noexcept { return terms_.begin(); }
	std::vector<TermResults<IdCont>>::const_iterator end() const noexcept { return terms_.end(); }

private:
	std::vector<TermResults<IdCont>> terms_;
};

template <typename IdCont>
struct [[nodiscard]] QueryMergeData {
	std::vector<PhraseOrTerm<IdCont>> queryParts;
	std::vector<Synonym<IdCont>> synonyms;
	size_t totalORVids = 0;

	void SortSubterms() {
		for (auto& qp : queryParts) {
			qp.SortSubterms();
		}

		for (auto& syn : synonyms) {
			for (auto& term : syn.Terms()) {
				term.SortSubterms();
			}
		}
	}

	bool Empty() const { return queryParts.size() == 0 || (queryParts.size() == 1 && queryParts[0].Op() == OpNot); }
	bool Simple() const { return queryParts.size() == 1 && queryParts[0].Op() != OpNot && queryParts[0].IsTerm() && synonyms.size() == 0; }
	bool Trivial() const { return Simple() && queryParts[0].Term().NumSubterms() == 1; }

	size_t QueryLength() const {
		size_t queryLength = 0;
		for (auto& qp : queryParts) {
			queryLength += qp.IsPhrase() ? qp.Phrase().NumTerms() : 1;
		}

		return queryLength;
	}

	void SupressDuplicatesInSynonyms() {
		FoundWordsType foundWords;

		for (const auto& qp : queryParts) {
			if (qp.IsTerm()) {
				for (const auto& subterm : qp.Term()) {
					++foundWords[subterm.PatternID()];
				}
			}
		}

		for (auto& syn : synonyms) {
			for (auto& term : syn) {
				for (auto& subterm : term) {
					if (foundWords.find(subterm.PatternID()) != foundWords.end()) {
						subterm.SetSuppressed(true);
					}
				}
			}
		}
	}
};

}  // namespace ft
}  // namespace reindexer
