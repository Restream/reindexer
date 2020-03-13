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
	QueryPreprocessor(const QueryEntries &queries, NamespaceImpl *ns) : QueryEntries(queries), ns_(*ns) {}
	const QueryEntries &GetQueryEntries() const { return *this; }

	void LookupQueryIndexes() {
		const size_t merged = lookupQueryIndexes(0, 0, Size());
		container_.resize(Size() - merged);
	}
	bool ContainsFullTextIndexes() const;
	void SubstituteCompositeIndexes() { substituteCompositeIndexes(0, Size()); }
	void ConvertWhereValues() { convertWhereValues(begin(), end()); }
	SortingEntries DetectOptimalSortOrder() const;
	void AddDistinctEntries(const h_vector<Aggregator, 4> &);

private:
	size_t lookupQueryIndexes(size_t dst, size_t srcBegin, size_t srcEnd);
	size_t substituteCompositeIndexes(size_t from, size_t to);
	KeyValueType detectQueryEntryIndexType(const QueryEntry &) const;
	bool mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs) const;
	int getCompositeIndex(const FieldsSet &) const;
	void convertWhereValues(QueryEntries::iterator begin, QueryEntries::iterator end) const;
	void convertWhereValues(QueryEntry *) const;
	const Index *findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const;

	NamespaceImpl &ns_;
};

}  // namespace reindexer
