#pragma once

#include "core/queryresults/tableviewbuilder.h"

namespace reindexer_tool {

class Output;

template <typename QueryResultsT>
class TableViewScroller {
public:
	TableViewScroller(const QueryResultsT& r, reindexer::TableViewBuilder<QueryResultsT>& tableBuilder, int linesOnPage);
	void Scroll(Output& o, const std::function<bool(void)>& isCanceled);

private:
	const QueryResultsT& r_;
	reindexer::TableViewBuilder<QueryResultsT>& tableBuilder_;
	int linesOnPage_;
};

void WaitEnterToContinue(std::ostream& o, int terminalWidth, const std::function<bool(void)>& isCanceled);
}  // namespace reindexer_tool
