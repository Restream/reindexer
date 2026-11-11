#pragma once

#include <cstdint>

namespace reindexer {

class NamespaceImpl;
class LocalQueryResults;
class RdxContext;
struct SelectCtx;
class [[nodiscard]] WALSelecter {
public:
	WALSelecter(const NamespaceImpl* ns, bool allowTxWithoutBegining);
	void operator()(LocalQueryResults& result, SelectCtx& params, bool snapshot = false);

protected:
	void putReplState(LocalQueryResults& result);
	const NamespaceImpl* ns_;
	const bool allowTxWithoutBegining_;
};

}  // namespace reindexer
