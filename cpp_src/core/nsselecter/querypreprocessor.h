#pragma once

#include "aggregator.h"
#include "core/ft/ftsetcashe.h"
#include "core/index/ft_preselect.h"
#include "core/query/queryentry.h"
#include "estl/h_vector.h"
#include "joinedselector.h"

namespace reindexer {

class Index;
class NamespaceImpl;
class SelectIteratorContainer;

class QueryPreprocessor : private QueryEntries {
public:
	QueryPreprocessor(QueryEntries &&, const Query &, NamespaceImpl *, bool reqMatchedOnce, bool inTransaction);
	const QueryEntries &GetQueryEntries() const noexcept { return *this; }
	void LookupQueryIndexes() {
		const size_t merged = lookupQueryIndexes(0, 0, container_.size() - queryEntryAddedByForcedSortOptimization_);
		if (queryEntryAddedByForcedSortOptimization_) {
			container_[container_.size() - merged - 1] = std::move(container_.back());
		}
		container_.resize(container_.size() - merged);
	}
	bool ContainsFullTextIndexes() const;
	bool ContainsForcedSortOrder() const noexcept {
		if (queryEntryAddedByForcedSortOptimization_) {
			return forcedStage();
		}
		return forcedSortOrder_;
	}
	void CheckUniqueFtQuery() const;
	void SubstituteCompositeIndexes() { substituteCompositeIndexes(0, container_.size() - queryEntryAddedByForcedSortOptimization_); }
	void ConvertWhereValues() { convertWhereValues(begin(), end()); }
	void AddDistinctEntries(const h_vector<Aggregator, 4> &);
	bool NeedNextEvaluation(unsigned start, unsigned count, bool &matchedAtLeastOnce) noexcept;
	unsigned Start() const noexcept { return start_; }
	unsigned Count() const noexcept { return count_; }
	bool MoreThanOneEvaluation() const noexcept { return queryEntryAddedByForcedSortOptimization_; }
	bool AvailableSelectBySortIndex() const noexcept { return !queryEntryAddedByForcedSortOptimization_ || !forcedStage(); }
	void InjectConditionsFromJoins(JoinedSelectors &js, const RdxContext &rdxCtx) {
		injectConditionsFromJoins(0, container_.size(), js, rdxCtx);
	}
	using QueryEntries::Size;
	using QueryEntries::Dump;
	SortingEntries GetSortingEntries(bool havePreresult) const;
	bool IsFtExcluded() const noexcept { return ftEntry_.has_value(); }
	void ExcludeFtQuery(const SelectFunction &, const RdxContext &);
	FtMergeStatuses &GetFtMergeStatuses() noexcept {
		assertrx(ftPreselect_);
		return std::get<FtMergeStatuses>(*ftPreselect_);
	}
	FtPreselectT &&MoveFtPreselect() noexcept {
		assertrx(ftPreselect_);
		return std::move(*ftPreselect_);
	}
	bool IsFtPreselected() const noexcept { return ftPreselect_ && !ftEntry_; }

private:
	SortingEntries detectOptimalSortOrder() const;
	bool forcedStage() const noexcept { return evaluationsCount_ == (desc_ ? 1 : 0); }
	size_t lookupQueryIndexes(size_t dst, size_t srcBegin, size_t srcEnd);
	size_t substituteCompositeIndexes(size_t from, size_t to);
	KeyValueType detectQueryEntryFieldType(const QueryEntry &qentry) const;
	bool mergeQueryEntries(size_t lhs, size_t rhs);
	int getCompositeIndex(const FieldsSet &) const;
	void convertWhereValues(QueryEntries::iterator begin, QueryEntries::iterator end) const;
	void convertWhereValues(QueryEntry *) const;
	const Index *findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const;
	void injectConditionsFromJoins(size_t from, size_t to, JoinedSelectors &, const RdxContext &);
	void checkStrictMode(const std::string &index, int idxNo) const;

	NamespaceImpl &ns_;
	StrictMode strictMode_;
	size_t evaluationsCount_ = 0;
	unsigned start_ = 0;
	unsigned count_ = UINT_MAX;
	bool queryEntryAddedByForcedSortOptimization_ = false;
	bool desc_ = false;
	bool forcedSortOrder_ = false;
	bool reqMatchedOnce_ = false;
	const Query &query_;
	std::optional<QueryEntry> ftEntry_;
	std::optional<FtPreselectT> ftPreselect_;
};

}  // namespace reindexer
