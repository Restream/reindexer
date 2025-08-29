#pragma once

#include <stdlib.h>
#include "core/query/query.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

class WrSerializer;
class Query;

class [[nodiscard]] SQLEncoder {
public:
	SQLEncoder(const Query& q) noexcept : SQLEncoder(q, q.Type()) {}
	SQLEncoder(const Query& q, QueryType queryType) noexcept : query_(q), realQueryType_(queryType) {}

	WrSerializer& GetSQL(WrSerializer& ser, bool stripArgs = false) const;

	/// Gets printable sql version of joined query set by idx.
	/// @param idx - index of joined query in joinQueries_.
	/// @param ser - serializer to store SQL string.
	/// @param stripArgs - replace condition values with '?'.
	void DumpSingleJoinQuery(size_t idx, WrSerializer& ser, bool stripArgs) const;

protected:
	/// Builds print version of a query with join in sql format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpJoined(WrSerializer& ser, bool stripArgs) const;

	/// Builds a print version of a query with merge queries in sql format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpMerged(WrSerializer& ser, bool stripArgs) const;

	/// Builds a print version of a query's order by statement
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpOrderBy(WrSerializer& ser, bool stripArgs) const;

	/// Builds a print version of all equal_position() functions in query.
	/// @param ser - serializer to store SQL string
	/// @param equalPositions - equal positions array
	void dumpEqualPositions(WrSerializer& ser, const EqualPositions_t& equalPositions) const;

	/// Builds a print version of all where condition entries.
	/// @param from - iterator to first entry
	/// @param to - iterator to last entry
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpWhereEntries(QueryEntries::const_iterator from, QueryEntries::const_iterator to, WrSerializer& ser, bool stripArgs) const;
	void dumpSQLWhere(WrSerializer& ser, bool stripArgs) const;

	const Query& query_;
	const QueryType realQueryType_;
};

}  // namespace reindexer
