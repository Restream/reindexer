#pragma once

#include <stdlib.h>
#include "core/type_consts.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

class WrSerializer;
class Query;

class SQLEncoder {
public:
	SQLEncoder(const Query &q);

	WrSerializer &GetSQL(WrSerializer &ser, bool stripArgs = false) const;

	/// Gets printable sql version of joined query set by idx.
	/// @param idx - index of joined query in joinQueries_.
	/// @param ser - serializer to store SQL string.
	/// @param stripArgs - replace condition values with '?'.
	void DumpSingleJoinQuery(size_t idx, WrSerializer &ser, bool stripArgs) const;

	/// Get  readaby Join Type
	/// @param type - join tyoe
	/// @return string with join type name
	static const char *JoinTypeName(JoinType type);

protected:
	/// Builds print version of a query with join in sql format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpJoined(WrSerializer &ser, bool stripArgs) const;

	/// Builds a print version of a query with merge queries in sql format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpMerged(WrSerializer &ser, bool stripArgs) const;

	/// Builds a print version of a query's order by statement
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpOrderBy(WrSerializer &ser, bool stripArgs) const;

	void dumpSQLWhere(WrSerializer &ser, bool stripArgs) const;

	const Query &query_;
};

}  // namespace reindexer
