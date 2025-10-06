#include "tableviewbuilder.h"

#include <wchar.h>
#include <iomanip>

#include "estl/gift_str.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "tools/terminalutils.h"
#include "vendor/gason/gason.h"
#include "vendor/utf8cpp/utf8.h"
#include "vendor/wcwidth/wcwidth.h"

namespace reindexer {

const std::string kSeparator = " | ";
const int kSuppositiveScreenWidth = 100;

bool ColumnData::IsNumber() const noexcept {
	return (gason::JsonTag(type) == gason::JsonTag::NUMBER) || (gason::JsonTag(type) == gason::JsonTag::DOUBLE);
}

bool ColumnData::IsBoolean() const noexcept {
	return (gason::JsonTag(type) == gason::JsonTag::JTRUE) || (gason::JsonTag(type) == gason::JsonTag::JFALSE);
}

bool ColumnData::PossibleToBreakTheLine() const noexcept {
	return IsBoolean() || (gason::JsonTag(type) == gason::JsonTag::STRING) /*|| (gason::JsonTag(type) == gason::JsonTag::OBJECT) */ ||
		   (gason::JsonTag(type) == gason::JsonTag::ARRAY);
}

TableCalculator::TableCalculator(std::vector<std::string>&& jsonData, int outputWidth) : outputWidth_(outputWidth) {
	calculate(std::move(jsonData));
}

void TableCalculator::calculate(std::vector<std::string>&& jsonData) {
	rows_.reserve(jsonData.size());
	for (auto&& json : jsonData) {
		gason::JsonParser parser;
		gason::JsonNode root = parser.Parse(reindexer::giftStr(json));
		Row rowData;

		for (auto& elem : root) {
			WrSerializer wrser;
			jsonValueToString(elem.value, wrser, 0, 0, false);
			std::string fieldValue = std::string(wrser.Slice());
			std::string fieldName = std::string(elem.key);
			ColumnData& columnData = columnsData_[fieldName];

			columnData.type = int(elem.value.getTag());
			columnData.maxWidthCh = std::max(columnData.maxWidthCh, reindexer::getStringTerminalWidth(fieldValue));
			if (columnData.entries == 0) {
				header_.push_back(fieldName);
				columnData.maxWidthCh = std::max(columnData.maxWidthCh, reindexer::getStringTerminalWidth(fieldName));
			}

			if (fieldValue.empty()) {
				++columnData.emptyValues;
			}
			columnData.entries++;
			rowData[fieldName] = fieldValue;
		}

		rows_.emplace_back(std::move(rowData));
		std::string empty;
		json.swap(empty);
	}
	{
		std::vector<std::string> empty;
		jsonData.swap(empty);
	}

	int currentLength = 0;
	for (auto it = header_.begin(); it != header_.end();) {
		std::string columnName = *it;
		ColumnData& columnData = columnsData_[columnName];
		if ((columnData.entries <= int(rows_.size() / 3)) || (columnData.emptyValues == int(rows_.size()))) {
			it = header_.erase(it);
			columnsData_.erase(columnName);
		} else {
			currentLength += columnData.maxWidthCh;
			currentLength += kSeparator.length();
			++it;
		}
	}

	bool needToRecalculateWidth = (currentLength > outputWidth_);

	int currPos = 0;
	size_t columnIdx = 0;
	for (auto it = header_.begin(); it != header_.end(); ++it, ++columnIdx) {
		ColumnData& columnData = columnsData_[*it];
		columnData.widthCh = columnData.maxWidthCh;
		if (needToRecalculateWidth) {
			if (header_.size() == 1) {
				columnData.widthCh = outputWidth_;
			} else {
				if ((columnIdx == header_.size() - 1) && (outputWidth_ > currPos)) {
					columnData.widthCh = outputWidth_ - currPos - ((kSeparator.length() * (columnIdx + 1)) - kSeparator.length());
				} else {
					double widthPercentage = (double(columnData.maxWidthCh) / outputWidth_) * 100;
					if (widthPercentage > 70.0) {
						if (header_.size() == 2) {
							columnData.widthCh = outputWidth_ * 0.7f;
						} else if (header_.size() == 3) {
							columnData.widthCh = outputWidth_ / 2;
						} else {
							columnData.widthCh = outputWidth_ / 3;
						}
					}
				}
			}
		}
		columnData.widthTerminalPercentage = (double(columnData.widthCh) / kSuppositiveScreenWidth) * 100;
		currPos += columnData.widthCh;
	}
}

void TableViewBuilder::Build(std::ostream& o, std::vector<std::string>&& jsonData, const std::function<bool(void)>& isCanceled) {
	if (isCanceled()) {
		return;
	}
	TerminalSize terminalSize = reindexer::getTerminalSize();
	TableCalculator tableCalculator(std::move(jsonData), terminalSize.width);
	BuildHeader(o, tableCalculator, isCanceled);
	BuildTable(o, tableCalculator, isCanceled);
}

void TableViewBuilder::BuildHeader(std::ostream& o, TableCalculator& tableCalculator, const std::function<bool(void)>& isCanceled) {
	if (isCanceled()) {
		return;
	}

	auto& header = tableCalculator.GetHeader();
	auto& columnsData = tableCalculator.GetColumnsSettings();

	size_t rowIdx = 0;
	const std::string headerLine(tableCalculator.GetOutputWidth(), '-');

	o << std::endl;
	o << headerLine << std::left;
	for (auto it = header.begin(); it != header.end(); ++it, ++rowIdx) {
		auto columnName = *it;
		auto& columnData = columnsData[columnName];
		ensureFieldWidthIsOk(columnName, columnData.widthCh);
		o << std::setw(computeFieldWidth(columnName, columnData.widthCh)) << columnName;
		if (rowIdx != header.size() - 1) {
			o << kSeparator;
		}
	}
	o << std::endl << headerLine << std::endl;
}

bool TableViewBuilder::isValueMultiline(std::string_view value, bool breakingTheLine, const ColumnData& columnData,
										int symbolsTillTheEOFLine) {
	return (breakingTheLine && columnData.PossibleToBreakTheLine() &&
			((symbolsTillTheEOFLine >= 4) || (symbolsTillTheEOFLine >= 2 && columnData.IsBoolean())) &&
			(double(getStringTerminalWidth(value)) / symbolsTillTheEOFLine <= 3));
}

void TableViewBuilder::startLine(std::ostream& o, const int& currLineWidth) {
	o << std::endl;
	for (size_t i = 0; i < currLineWidth - kSeparator.length(); ++i) {
		o << " ";
	}
	o << kSeparator;
}

void TableViewBuilder::BuildRow(std::ostream& o, int idx, TableCalculator& tableCalculator) {
	auto& columnsData = tableCalculator.GetColumnsSettings();
	auto& header = tableCalculator.GetHeader();

	size_t columnIdx = 0;
	int currLineWidth = 0;
	auto& row = tableCalculator.GetRows()[idx];
	for (auto it = header.begin(); it != header.end(); ++it, ++columnIdx) {
		const std::string& columnName = *it;
		auto& columnData = columnsData[columnName];
		std::string& value = row[columnName];

		ensureFieldWidthIsOk(value, columnData.widthCh);

		const int symbolsTillTheEOFLine = tableCalculator.GetOutputWidth() - currLineWidth;
		bool lastColumn = (columnIdx == header.size() - 1);
		bool breakingTheLine = (currLineWidth + columnData.widthCh > tableCalculator.GetOutputWidth());
		bool mutliLineValue = isValueMultiline(value, breakingTheLine, columnData, symbolsTillTheEOFLine);

		if (mutliLineValue) {
			int sz = 0, count = 0;
			int pos = 0, total = 0;
			int currWidth = 0;

			const char* cstr = value.c_str();
			const char* end = cstr + value.length();

			for (wchar_t wc; (sz = std::mbtowc(&wc, cstr, end - cstr)) > 0; cstr += sz) {
				currWidth += mk_wcwidth(wc);
				if (currWidth >= symbolsTillTheEOFLine) {
					if (pos != 0) {
						startLine(o, currLineWidth);
					}
					o << std::left;
					o << value.substr(pos, count);
					pos = total;
					currWidth = count = 0;
				}
				count += sz;
				total += sz;
			}

			if (count > 0) {
				if (pos != 0) {
					startLine(o, currLineWidth);
				}
				o << value.substr(pos, count);
			}
		} else {
			o << std::setw(computeFieldWidth(value, columnData.widthCh));
			if (columnData.IsNumber() && columnIdx && !lastColumn) {
				o << std::right;
			} else {
				o << std::left;
			}
			o << value;
		}
		if (!lastColumn) {
			if (breakingTheLine) {
				currLineWidth = (currLineWidth + columnData.widthCh) - tableCalculator.GetOutputWidth();
				currLineWidth += kSeparator.length();
				if (mutliLineValue) {
					startLine(o, currLineWidth);
				} else {
					o << kSeparator;
				}
			} else {
				currLineWidth += columnData.widthCh;
				currLineWidth += kSeparator.length();
				o << kSeparator;
			}
		}
	}
	o << std::endl;
}

void TableViewBuilder::BuildTable(std::ostream& o, TableCalculator& tableCalculator, const std::function<bool(void)>& isCanceled) {
	if (isCanceled()) {
		return;
	}
	auto& rows = tableCalculator.GetRows();
	for (size_t i = 0; i < rows.size(); ++i) {
		if (isCanceled()) {
			return;
		}
		BuildRow(o, i, tableCalculator);
	}
}

int TableViewBuilder::computeFieldWidth(std::string_view str, int maxWidth) {
	int terminalWidth = getStringTerminalWidth(str) + (maxWidth - str.length());
	int delta = maxWidth - terminalWidth;
	if (delta > 0) {
		return maxWidth + delta;
	}
	return maxWidth;
}

void TableViewBuilder::ensureFieldWidthIsOk(std::string& str, int maxWidth) {
	int width = getStringTerminalWidth(str);
	if (width > maxWidth) {
		int n = 0;
		int sz = 0;
		int newWidth = 0;
		static const std::string dots = " ...";
		bool withDots = (maxWidth > 10);
		if (withDots) {
			maxWidth -= dots.length();
		}
		try {
			for (auto it = str.begin(); it != str.end() && (sz = utf8::internal::sequence_length(it)) > 0;) {
				newWidth += mk_wcwidth(utf8::next(it, str.end()));
				if (newWidth > maxWidth) {
					break;
				}
				n += sz;
			}
		} catch (const std::exception&) {
			// str is not a proper UTF8 string
			n = maxWidth;
		}
		str = str.substr(0, n);
		if (withDots) {
			str += dots;
		}
	}
}

}  // namespace reindexer
