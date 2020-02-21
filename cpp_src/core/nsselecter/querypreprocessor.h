#pragma once

#include "core/query/queryentry.h"
#include "estl/h_vector.h"

namespace reindexer {

class Index;
class NamespaceImpl;
class SelectIteratorContainer;

class QueryPreprocessor {
public:
	QueryPreprocessor(NamespaceImpl *ns, QueryEntries *queries) : ns_(*ns), queries_(queries) {}
	void SetQueryEntries(QueryEntries *queries) { queries_ = queries; }
	const QueryEntries &GetQueryEntries() const { return *queries_; }

	QueryEntries LookupQueryIndexes() const;
	bool ContainsFullTextIndexes() const;
	void SubstituteCompositeIndexes() const { substituteCompositeIndexes(0, queries_->Size()); }
	void ConvertWhereValues() const { convertWhereValues(queries_->begin(), queries_->end()); }
	SortingEntries DetectOptimalSortOrder() const;

private:
	void lookupQueryIndexes(QueryEntries *dst, QueryEntries::const_iterator srcBegin, QueryEntries::const_iterator srcEnd) const;
	size_t substituteCompositeIndexes(size_t from, size_t to) const;
	KeyValueType detectQueryEntryIndexType(const QueryEntry &) const;
	bool mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs) const;
	int getCompositeIndex(const FieldsSet &) const;
	void convertWhereValues(QueryEntries::iterator begin, QueryEntries::iterator end) const;
	void convertWhereValues(QueryEntry *) const;
	const Index *findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const;

	NamespaceImpl &ns_;
	QueryEntries *queries_;
};

}  // namespace reindexer
