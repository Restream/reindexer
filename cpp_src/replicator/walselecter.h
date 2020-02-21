#pragma once

namespace reindexer {

class NamespaceImpl;
class QueryResults;
struct SelectCtx;
class WALSelecter {
public:
	WALSelecter(const NamespaceImpl *ns);
	void operator()(QueryResults &result, SelectCtx &params);

protected:
	void putReplState(QueryResults &result);
	const NamespaceImpl *ns_;
};

}  // namespace reindexer
