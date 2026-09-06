#pragma once

#include "core/query/query.h"

namespace reindexer {

inline void ApplySqlModifyDefaults(Query& q) {
	if ((q.Type() == QueryDelete || q.Type() == QueryUpdate) && q.SelectFilters().Empty()) {
		q.SelectAllFields();
	}
}

}  // namespace reindexer
