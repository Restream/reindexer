#pragma once

namespace reindexer {

class Namespace;
class QueryResults;
struct SelectCtx;
class WALSelecter {
public:
	WALSelecter(Namespace *ns);
	void operator()(QueryResults &result, SelectCtx &params);

protected:
	void putReplState(QueryResults &result);
	Namespace *ns_;
};

};  // namespace reindexer
