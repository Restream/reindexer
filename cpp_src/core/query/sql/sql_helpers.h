#pragma once

#include "core/query/query_impl.h"

namespace reindexer {

inline void ApplySqlModifyDefaults(Query& query) {
	const auto q = Impl(query);
	if ((q.Type() == QueryDelete || q.Type() == QueryUpdate) && q.SelectFilters().Empty()) {
		query.SelectAllFields();
	}
}

}  // namespace reindexer
