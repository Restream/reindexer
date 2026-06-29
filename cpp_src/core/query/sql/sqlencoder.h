#pragma once

#include <stdlib.h>
#include "core/query/query_impl.h"
#include "core/query/queryentry.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

class SingleLineSqlFormatter;

template <typename Formatter = SingleLineSqlFormatter>
class [[nodiscard]] SQLEncoder {
public:
	SQLEncoder(const impl::Query& q, Formatter& formatter) noexcept : SQLEncoder(q, q.Type(), formatter) {}
	SQLEncoder(const impl::Query& q, QueryType queryType, Formatter& formatter) noexcept
		: query_(q), realQueryType_(queryType), formatter_(formatter) {}

	void DumpSQL(bool stripArgs = false) const;

	/// Gets printable sql version of joined query set by idx.
	/// @param idx - index of joined query in joinQueries_.
	/// @param ser - serializer to store SQL string.
	/// @param stripArgs - replace condition values with '?'.
	void DumpSingleJoinQuery(size_t idx, bool stripArgs) const;

private:
	/// Builds print version of a query with join in sql format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpJoined(bool stripArgs) const;

	/// Builds a print version of a query with merge queries in sql format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpMerged(bool stripArgs) const;

	/// Builds a print version of a query's order by statement
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpOrderBy(bool stripArgs) const;

	/// Builds a print version of all equal_position() functions in query.
	/// @param ser - serializer to store SQL string
	/// @param equalPositions - equal positions array
	void dumpEqualPositions(const EqualPositions_t& equalPositions) const;

	/// Builds a print version of all where condition entries.
	/// @param from - iterator to first entry
	/// @param to - iterator to last entry
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpWhereEntries(QueryEntries::const_iterator from, QueryEntries::const_iterator to, bool stripArgs) const;
	void dumpSQLWhere(bool stripArgs) const;
	void printField(bool& needComma, std::string_view name) const;

	const impl::Query& query_;
	const QueryType realQueryType_;
	Formatter& formatter_;
};

}  // namespace reindexer
