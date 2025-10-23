#pragma once

#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include "estl/elist.h"
#include "tools/terminalutils.h"

namespace reindexer {

struct [[nodiscard]] ColumnData {
	int type = 0;
	int entries = 0;
	int widthCh = 0;
	int maxWidthCh = 0;
	int emptyValues = 0;
	double widthTerminalPercentage = 0;
	bool IsNumber() const noexcept;
	bool IsBoolean() const noexcept;
	bool PossibleToBreakTheLine() const noexcept;
};

class [[nodiscard]] TableCalculator {
public:
	using Header = elist<std::string>;
	using Row = std::unordered_map<std::string, std::string>;
	using Rows = std::vector<Row>;
	using ColumnsData = std::unordered_map<std::string, ColumnData>;

	TableCalculator(std::vector<std::string>&& jsonData, int outputWidth);

	Header& GetHeader() noexcept { return header_; }
	Rows& GetRows() noexcept { return rows_; }
	ColumnsData& GetColumnsSettings() noexcept { return columnsData_; }
	int GetOutputWidth() const noexcept { return outputWidth_; }

private:
	void calculate(std::vector<std::string>&& jsonData);

	Header header_;
	Rows rows_;
	TerminalSize terminalSize_;
	ColumnsData columnsData_;
	const int outputWidth_;
};

class [[nodiscard]] TableViewBuilder {
public:
	TableViewBuilder() = default;
	TableViewBuilder(const TableViewBuilder&) = delete;
	TableViewBuilder(TableViewBuilder&&) = delete;
	TableViewBuilder operator=(const TableViewBuilder&) = delete;
	TableViewBuilder operator=(TableViewBuilder&&) = delete;

	void Build(std::ostream& o, std::vector<std::string>&& jsonData, const std::function<bool(void)>& isCanceled);

	void BuildHeader(std::ostream& o, TableCalculator& tableCalculator, const std::function<bool(void)>& isCanceled);
	void BuildTable(std::ostream& o, TableCalculator& tableCalculator, const std::function<bool(void)>& isCanceled);
	void BuildRow(std::ostream& o, int idx, TableCalculator& tableCalculator);

private:
	static int computeFieldWidth(std::string_view str, int maxWidth);
	static void ensureFieldWidthIsOk(std::string& str, int maxWidth);
	static bool isValueMultiline(std::string_view, bool breakingTheLine, const ColumnData&, int symbolsTillTheEOFLine);
	static void startLine(std::ostream& o, const int& currLineWidth);
};

}  // namespace reindexer
