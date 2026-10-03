#pragma once

#include "core/queryresults/tableviewbuilder.h"

namespace rx_tv = reindexer::table_view;

namespace reindexer_tool {

class Output;

class [[nodiscard]] TableViewScroller {
public:
	TableViewScroller(rx_tv::TableViewBuilder& tableBuilder, int linesOnPage);
	void Scroll(Output& o, std::vector<std::string>&& jsonData, const std::function<bool(void)>& isCanceled);

private:
	rx_tv::TableViewBuilder& tableBuilder_;
	int linesOnPage_;
};

void WaitEnterToContinue(std::ostream& o, int terminalWidth, const std::function<bool(void)>& isCanceled);
}  // namespace reindexer_tool
