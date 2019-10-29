#pragma once

#include <iostream>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>
#include "tools/errors.h"
#include "tools/terminalutils.h"

namespace reindexer {

struct ColumnData {
	int type = 0;
	int entries = 0;
	int widthCh = 0;
	int maxWidthCh = 0;
	int emptyValues = 0;
	double widthTerminalPercentage = 0;
	bool IsNumber() const;
	bool IsBoolean() const;
	bool PossibleToBreakTheLine() const;
};

template <typename QueryResultsT>
class TableCalculator {
public:
	using Header = std::list<std::string>;
	using Row = std::unordered_map<std::string, std::string>;
	using Rows = std::vector<Row>;
	using ColumnsData = std::unordered_map<std::string, ColumnData>;

	TableCalculator(const QueryResultsT& r, int outputWidth);

	Header& GetHeader();
	Rows& GetRows();
	ColumnsData& GetColumnsSettings();
	int GetOutputWidth() const;

private:
	void calculate();

	const QueryResultsT& r_;
	Header header_;
	Rows rows_;
	TerminalSize terminalSize_;
	ColumnsData columnsData_;
	const int outputWidth_;
};

template <typename QueryResultsT>
class TableViewBuilder {
public:
	explicit TableViewBuilder(const QueryResultsT& r);
	TableViewBuilder(const TableViewBuilder&) = delete;
	TableViewBuilder(TableViewBuilder&&) = delete;
	TableViewBuilder operator=(const TableViewBuilder&) = delete;
	TableViewBuilder operator=(TableViewBuilder&&) = delete;

	void Build(std::ostream& o, const std::function<bool(void)>& isCanceled);

	void BuildHeader(std::ostream& o, TableCalculator<QueryResultsT>& tableCalculator, const std::function<bool(void)>& isCanceled);
	void BuildTable(std::ostream& o, TableCalculator<QueryResultsT>& tableCalculator, const std::function<bool(void)>& isCanceled);
	void BuildRow(std::ostream& o, int idx, TableCalculator<QueryResultsT>& tableCalculator);

private:
	static int computeFieldWidth(string_view str, int maxWidth);
	static void ensureFieldWidthIsOk(std::string& str, int maxWidth);
	static bool isValueMultiline(string_view, bool breakingTheLine, const ColumnData&, int symbolsTillTheEOFLine);
	static void startLine(std::ostream& o, const int& currLineWidth);

private:
	const QueryResultsT& r_;
};

}  // namespace reindexer
