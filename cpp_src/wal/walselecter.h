#pragma once

#include <cstdint>

namespace reindexer {

class NamespaceImpl;
class QueryResults;
class RdxContext;
struct SelectCtx;
class WALSelecter {
public:
	WALSelecter(const NamespaceImpl *ns, bool allowTxWithoutBegining);
	void operator()(QueryResults &result, SelectCtx &params, bool snapshot = false);

protected:
	void putReplState(QueryResults &result);
	const NamespaceImpl *ns_;
	const bool allowTxWithoutBegining_;
};

}  // namespace reindexer
