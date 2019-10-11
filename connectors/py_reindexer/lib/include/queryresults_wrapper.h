#pragma once

#ifdef PYREINDEXER_CPROTO
#include "client/reindexer.h"
#else
#include "core/reindexer.h"
#endif

namespace pyreindexer {

#ifdef PYREINDEXER_CPROTO
using reindexer::client::QueryResults;
#else
using reindexer::QueryResults;
#endif

struct QueryResultsWrapper {
	void iterInit() { itPtr = qresPtr.begin(); }

	void next() {
		++itPtr;
		if (itPtr == qresPtr.end()) {
			iterInit();
		}
	}

	QueryResults qresPtr;
	QueryResults::Iterator itPtr;
};

}  // namespace pyreindexer
