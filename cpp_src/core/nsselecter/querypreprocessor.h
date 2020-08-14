#pragma once

#include "aggregator.h"
#include "core/query/queryentry.h"
#include "estl/h_vector.h"

namespace reindexer {

class Index;
class NamespaceImpl;
class SelectIteratorContainer;

class QueryPreprocessor : private QueryEntries {
public:
	QueryPreprocessor(QueryEntries &&, const Query &, NamespaceImpl *, bool reqMatchedOnce);
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
	void SubstituteCompositeIndexes() { substituteCompositeIndexes(0, container_.size() - queryEntryAddedByForcedSortOptimization_); }
	void ConvertWhereValues() { convertWhereValues(begin(), end()); }
	SortingEntries DetectOptimalSortOrder() const;
	void AddDistinctEntries(const h_vector<Aggregator, 4> &);
	bool NeedNextEvaluation(unsigned start, unsigned count, bool matchedAtLeastOnce) noexcept {
		if (evaluationsCount_++ || !queryEntryAddedByForcedSortOptimization_) return false;
		container_.back().operation = desc_ ? OpAnd : OpNot;
		assert(start <= start_);
		start_ = start;
		assert(count <= count_);
		count_ = count;
		return count_ || (reqMatchedOnce_ && !matchedAtLeastOnce);
	}
	unsigned Start() const noexcept { return start_; }
	unsigned Count() const noexcept { return count_; }
	bool MoreThanOneEvaluation() const noexcept { return queryEntryAddedByForcedSortOptimization_; }
	bool AvailableSelectBySortIndex() const noexcept { return !queryEntryAddedByForcedSortOptimization_ || !forcedStage(); }
	using QueryEntries::Size;

private:
	bool forcedStage() const noexcept { return evaluationsCount_ == (desc_ ? 1 : 0); }
	size_t lookupQueryIndexes(size_t dst, size_t srcBegin, size_t srcEnd);
	size_t substituteCompositeIndexes(size_t from, size_t to);
	KeyValueType detectQueryEntryIndexType(const QueryEntry &) const;
	bool mergeQueryEntries(size_t lhs, size_t rhs);
	int getCompositeIndex(const FieldsSet &) const;
	void convertWhereValues(QueryEntries::iterator begin, QueryEntries::iterator end) const;
	void convertWhereValues(QueryEntry *) const;
	const Index *findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const;

	NamespaceImpl &ns_;
	StrictMode strictMode_;
	size_t evaluationsCount_ = 0;
	unsigned start_ = 0;
	unsigned count_ = UINT_MAX;
	bool queryEntryAddedByForcedSortOptimization_ = false;
	bool desc_ = false;
	bool forcedSortOrder_ = false;
	bool reqMatchedOnce_ = false;
};

}  // namespace reindexer
