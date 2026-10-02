#include "sortingcontext.h"
#include "core/query/queryentry.h"
#include "core/sorting/reranker.h"

namespace reindexer {

reindexer::Desc SortingContext::Entry::Desc() const {
	return std::visit([](const auto& e) noexcept { return e.data.desc; }, AsVariant());
}

Reranker SortingContext::ToReranker(const NamespaceImpl& ns) const {
	if (expressions.empty() && entries.empty()) {
		return Reranker::Default();
	} else {
		if (expressions.size() != 1 || entries.size() != 1) {
			SortExpression::ThrowNonReranker();
		}
		return expressions[0].ToReranker(ns, entries[0].Desc());
	}
}

}  // namespace reindexer
