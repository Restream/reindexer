#pragma once

namespace reindexer {

class Namespace;
class QueryResults;
struct SelectCtx;
class WALSelecter {
public:
	WALSelecter(const Namespace *ns);
	void operator()(QueryResults &result, SelectCtx &params);

protected:
	void putReplState(QueryResults &result);
	const Namespace *ns_;
};

}  // namespace reindexer
