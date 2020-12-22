#include "sortexpression.h"
#include <set>
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "joinedselector.h"
#include "joinedselectormock.h"
#include "tools/stringstools.h"
#include "vendor/double-conversion/double-conversion.h"

namespace {

static void throwParseError(reindexer::string_view sortExpr, reindexer::string_view::iterator pos, reindexer::string_view message) {
	throw reindexer::Error(errParams, "'%s' is not valid sort expression. Parser failed at position %d.%s%s", sortExpr,
						   pos - sortExpr.begin(), message.empty() ? "" : " ", message);
}

static inline double distance(reindexer::Point p1, reindexer::Point p2) noexcept {
	return std::sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
}

static reindexer::VariantArray getFieldValues(reindexer::ConstPayload pv, reindexer::TagsMatcher& tagsMatcher, int index,
											  reindexer::string_view column) {
	reindexer::VariantArray values;
	if (index == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(column, tagsMatcher, values, KeyValueUndefined);
	} else {
		pv.Get(index, values);
	}
	return values;
}

}  // namespace

namespace reindexer {

using SortExprFuncs::Value;
using SortExprFuncs::JoinedIndex;
using SortExprFuncs::Rank;
using SortExprFuncs::DistanceFromPoint;
using SortExprFuncs::DistanceJoinedIndexFromPoint;
using SortExprFuncs::DistanceBetweenIndexes;
using SortExprFuncs::DistanceBetweenJoinedIndexes;
using SortExprFuncs::DistanceBetweenIndexAndJoinedIndex;
using SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;

ItemImpl SortExpression::getJoinedItem(IdType rowId, const joins::NamespaceResults& joinResults,
									   const std::vector<JoinedSelector>& joinedSelectors, size_t nsIdx) {
	assert(joinedSelectors.size() > nsIdx);
	const auto& js = joinedSelectors[nsIdx];
	const joins::ItemIterator jIt{&joinResults, rowId};
	const auto jfIt = jIt.at(nsIdx);
	if (jfIt == jIt.end() || jfIt.ItemsCount() == 0) throw Error(errQueryExec, "Not found value joined from ns %s", js.RightNsName());
	if (jfIt.ItemsCount() > 1) throw Error(errQueryExec, "Found more than 1 value joined from ns %s", js.RightNsName());
	const PayloadType& pt =
		js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.payloadType : js.rightNs_->payloadType_;
	TagsMatcher& tm = js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.tagsMatcher : js.rightNs_->tagsMatcher_;
	return jfIt.GetItem(0, pt, tm);
}

VariantArray SortExpression::getJoinedFieldValues(IdType rowId, const joins::NamespaceResults& joinResults,
												  const std::vector<JoinedSelector>& joinedSelectors, size_t nsIdx, string_view column,
												  int index) {
	const auto& js = joinedSelectors[nsIdx];
	const PayloadType& pt =
		js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.payloadType : js.rightNs_->payloadType_;
	ItemImpl item{getJoinedItem(rowId, joinResults, joinedSelectors, nsIdx)};
	const ConstPayload pv{pt, item.Value()};
	TagsMatcher& tm = js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.tagsMatcher : js.rightNs_->tagsMatcher_;
	VariantArray values;
	if (index == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(column, tm, values, KeyValueUndefined);
	} else {
		pv.Get(index, values);
	}
	return values;
}

bool SortExpression::ByIndexField() const {
	static constexpr SortExpressionOperation noOperation;
	return Size() == 1 && IsValue(0) && container_[0].Holds<SortExprFuncs::Index>() && GetOperation(0) == noOperation;
}

bool SortExpression::ByJoinedIndexField() const {
	static constexpr SortExpressionOperation noOperation;
	return Size() == 1 && IsValue(0) && container_[0].Holds<JoinedIndex>() && GetOperation(0) == noOperation;
}

double SortExprFuncs::Index::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values = getFieldValues(pv, tagsMatcher, index, column);
	if (values.empty()) throw Error(errQueryExec, "Empty field in sort expression: %s", column);
	if (values.size() > 1 || values[0].Type() == KeyValueComposite || values[0].Type() == KeyValueTuple) {
		throw Error(errQueryExec, "Array, composite or tuple field in sort expression");
	}
	return values[0].As<double>();
}

double DistanceFromPoint::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values = getFieldValues(pv, tagsMatcher, index, column);
	return distance(static_cast<Point>(values), point);
}

double JoinedIndex::GetValue(IdType rowId, const joins::NamespaceResults& joinResults,
							 const std::vector<JoinedSelector>& joinedSelectors) const {
	const VariantArray values = SortExpression::getJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx, column, index);
	if (values.empty()) throw Error(errQueryExec, "Empty field in sort expression: %s %s", static_cast<int>(nsIdx), column);
	if (values.size() > 1 || values[0].Type() == KeyValueComposite || values[0].Type() == KeyValueTuple) {
		throw Error(errQueryExec, "Array, composite or tuple field in sort expression");
	}
	return values[0].As<double>();
}

double DistanceJoinedIndexFromPoint::GetValue(IdType rowId, const joins::NamespaceResults& joinResults,
											  const std::vector<JoinedSelector>& joinedSelectors) const {
	const VariantArray values = SortExpression::getJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx, column, index);
	return distance(static_cast<Point>(values), point);
}

double DistanceBetweenIndexes::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values1 = getFieldValues(pv, tagsMatcher, index1, column1);
	const VariantArray values2 = getFieldValues(pv, tagsMatcher, index2, column2);
	return distance(static_cast<Point>(values1), static_cast<Point>(values2));
}

double DistanceBetweenIndexAndJoinedIndex::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher, IdType rowId,
													const joins::NamespaceResults& joinResults,
													const std::vector<JoinedSelector>& joinedSelectors) const {
	const VariantArray values1 = getFieldValues(pv, tagsMatcher, index, column);
	const VariantArray values2 = SortExpression::getJoinedFieldValues(rowId, joinResults, joinedSelectors, jNsIdx, jColumn, jIndex);
	return distance(static_cast<Point>(values1), static_cast<Point>(values2));
}

double DistanceBetweenJoinedIndexes::GetValue(IdType rowId, const joins::NamespaceResults& joinResults,
											  const std::vector<JoinedSelector>& joinedSelectors) const {
	const VariantArray values1 = SortExpression::getJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx1, column1, index1);
	const VariantArray values2 = SortExpression::getJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx2, column2, index2);
	return distance(static_cast<Point>(values1), static_cast<Point>(values2));
}

double DistanceBetweenJoinedIndexesSameNs::GetValue(IdType rowId, const joins::NamespaceResults& joinResults,
													const std::vector<JoinedSelector>& joinedSelectors) const {
	const auto& js = joinedSelectors[nsIdx];
	const PayloadType& pt =
		js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.payloadType : js.rightNs_->payloadType_;
	ItemImpl item{SortExpression::getJoinedItem(rowId, joinResults, joinedSelectors, nsIdx)};
	const ConstPayload pv{pt, item.Value()};
	TagsMatcher& tm = js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.tagsMatcher : js.rightNs_->tagsMatcher_;
	VariantArray values1;
	if (index1 == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(column1, tm, values1, KeyValueUndefined);
	} else {
		pv.Get(index1, values1);
	}
	VariantArray values2;
	if (index2 == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(column2, tm, values2, KeyValueUndefined);
	} else {
		pv.Get(index2, values2);
	}
	return distance(static_cast<Point>(values1), static_cast<Point>(values2));
}

void SortExpression::openBracketBeforeLastAppended() {
	const size_t pos = lastAppendedElement();
	assert(activeBrackets_.empty() || activeBrackets_.back() < pos);
	for (unsigned i : activeBrackets_) {
		assert(i < container_.size());
		container_[i].Append();
	}
	const ArithmeticOpType op = container_[pos].operation.op;
	container_[pos].operation.op = OpPlus;
	activeBrackets_.push_back(pos);
	container_.insert(container_.begin() + pos, {{op, false}, container_.size() - pos + 1});
}

template <typename T>
struct ParseIndexNameResult {
	typename std::vector<T>::const_iterator joinedSelectorIt;
	string_view name;
};

template <typename T>
static ParseIndexNameResult<T> parseIndexName(string_view::iterator& it, const string_view::iterator end,
											  const std::vector<T>& joinedSelectors, const string_view fullExpr) {
	static const std::set<char> allowedSymbolsInIndexName{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
		'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
		'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', '+'};

	auto start = it;
	auto joinedSelectorIt = joinedSelectors.cend();
	while (it != end && *it != '.' && allowedSymbolsInIndexName.find(*it) != allowedSymbolsInIndexName.end()) ++it;
	if (it != end && *it == '.') {
		const string_view namespaceName = {start, static_cast<size_t>(it - start)};
		++it;
		joinedSelectorIt = std::find_if(joinedSelectors.cbegin(), joinedSelectors.cend(),
										[namespaceName](const T& js) { return namespaceName == js.RightNsName(); });
		if (joinedSelectorIt != joinedSelectors.cend()) {
			if (std::find_if(joinedSelectorIt + 1, joinedSelectors.cend(),
							 [namespaceName](const T& js) { return namespaceName == js.RightNsName(); }) != joinedSelectors.cend()) {
				throwParseError(fullExpr, it,
								"Sorting by namespace which has been joined more than once: '" + string(namespaceName) + "'.");
			}
			start = it;
		}
	}
	while (it != end && allowedSymbolsInIndexName.find(*it) != allowedSymbolsInIndexName.end()) ++it;
	const string_view name{start, static_cast<size_t>(it - start)};
	if (name.empty()) {
		throwParseError(fullExpr, it, "Expected index or function name.");
	}
	return {joinedSelectorIt, name};
}

template <typename SkipWS>
static Point parsePoint(string_view::iterator& it, string_view::iterator end, string_view funcName, string_view fullExpr,
						const SkipWS& skipSpaces) {
	using namespace double_conversion;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_TRAILING_JUNK |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   0.0, 0.0, nullptr, nullptr};
	if (funcName != "st_geomfromtext") {
		throwParseError(fullExpr, it, "Unsupported function inside ST_Distance() : '" + std::string(funcName) + "'.");
	}
	++it;
	skipSpaces();
	if (it == end || (*it != '\'' && *it != '"')) throwParseError(fullExpr, it, "Expected \" or '.");
	const char openQuote = *it;
	++it;
	skipSpaces();
	if (!checkIfStartsWith("point"_sv, {it, static_cast<size_t>(end - it)})) throwParseError(fullExpr, it, "Expected 'point'.");
	it += 5;
	skipSpaces();
	if (it == end || *it != '(') throwParseError(fullExpr, it, "Expected '('.");
	++it;
	skipSpaces();
	int countOfCharsParsedAsDouble = 0;
	const double x = converter.StringToDouble(it, end - it, &countOfCharsParsedAsDouble);
	if (countOfCharsParsedAsDouble == 0) throwParseError(fullExpr, it, "Expected number.");
	it += countOfCharsParsedAsDouble;
	skipSpaces();
	countOfCharsParsedAsDouble = 0;
	const double y = converter.StringToDouble(it, end - it, &countOfCharsParsedAsDouble);
	if (countOfCharsParsedAsDouble == 0) throwParseError(fullExpr, it, "Expected number.");
	it += countOfCharsParsedAsDouble;
	skipSpaces();
	if (it == end || *it != ')') throwParseError(fullExpr, it, "Expected ')'.");
	++it;
	skipSpaces();
	if (it == end || *it != openQuote) throwParseError(fullExpr, it, std::string("Expected ") + openQuote + '.');
	++it;
	skipSpaces();
	if (it == end || *it != ')') throwParseError(fullExpr, it, "Expected ')'.");
	++it;
	return {x, y};
}

template <typename T, typename SkipSW>
void SortExpression::parseDistance(string_view::iterator& it, string_view::iterator end, const std::vector<T>& joinedSelectors,
								   string_view fullExpr, const ArithmeticOpType op, const bool negative, const SkipSW& skipSpaces) {
	skipSpaces();
	const auto parsedIndexName1 = parseIndexName(it, end, joinedSelectors, fullExpr);
	skipSpaces();
	if (parsedIndexName1.joinedSelectorIt != joinedSelectors.cend()) {
		if (it == end || *it != ',') throwParseError(fullExpr, it, "Expected ','.");
		++it;
		skipSpaces();
		const size_t jNsIdx1 = static_cast<size_t>(parsedIndexName1.joinedSelectorIt - joinedSelectors.cbegin());
		const auto parsedIndexName2 = parseIndexName(it, end, joinedSelectors, fullExpr);
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			if (parsedIndexName1.joinedSelectorIt == parsedIndexName2.joinedSelectorIt) {
				if (toLower(parsedIndexName1.name) == toLower(parsedIndexName2.name))
					throwParseError(fullExpr, it, "Distance between two same indexes");
				Append({op, negative}, DistanceBetweenJoinedIndexesSameNs{jNsIdx1, parsedIndexName1.name, parsedIndexName2.name});
			} else {
				Append({op, negative},
					   DistanceBetweenJoinedIndexes{jNsIdx1, parsedIndexName1.name,
													static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
													parsedIndexName2.name});
			}
		} else {
			skipSpaces();
			if (it != end && *it == '(') {
				const auto point = parsePoint(it, end, toLower(parsedIndexName2.name), fullExpr, skipSpaces);
				Append({op, negative}, DistanceJoinedIndexFromPoint{jNsIdx1, parsedIndexName1.name, point});
			} else {
				Append({op, negative}, DistanceBetweenIndexAndJoinedIndex{parsedIndexName2.name, jNsIdx1, parsedIndexName1.name});
			}
		}
	} else if (it != end && *it == '(') {
		const auto point = parsePoint(it, end, toLower(parsedIndexName1.name), fullExpr, skipSpaces);
		skipSpaces();
		if (it == end || *it != ',') throwParseError(fullExpr, it, "Expected ','.");
		++it;
		skipSpaces();
		const auto parsedIndexName2 = parseIndexName(it, end, joinedSelectors, fullExpr);
		skipSpaces();
		if (it != end && *it == '(') throwParseError(fullExpr, it, "Allowed only one function inside ST_Geometry");
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			Append({op, negative},
				   DistanceJoinedIndexFromPoint{static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
												parsedIndexName2.name, point});
		} else {
			Append({op, negative}, DistanceFromPoint{parsedIndexName2.name, point});
		}
	} else {
		if (it == end || *it != ',') throwParseError(fullExpr, it, "Expected ','.");
		++it;
		skipSpaces();
		const auto parsedIndexName2 = parseIndexName(it, end, joinedSelectors, fullExpr);
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			Append({op, negative},
				   DistanceBetweenIndexAndJoinedIndex{parsedIndexName1.name,
													  static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
													  parsedIndexName2.name});
		} else {
			skipSpaces();
			if (it != end && *it == '(') {
				const auto point = parsePoint(it, end, toLower(parsedIndexName2.name), fullExpr, skipSpaces);
				Append({op, negative}, DistanceFromPoint{parsedIndexName1.name, point});
			} else {
				if (toLower(parsedIndexName1.name) == toLower(parsedIndexName2.name))
					throwParseError(fullExpr, it, "Distance between two same indexes");
				Append({op, negative}, DistanceBetweenIndexes{parsedIndexName1.name, parsedIndexName2.name});
			}
		}
	}
	skipSpaces();
}

template <typename T>
string_view::iterator SortExpression::parse(string_view::iterator it, const string_view::iterator end, bool* containIndexOrFunction,
											string_view fullExpr, const std::vector<T>& joinedSelectors) {
	using namespace double_conversion;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_TRAILING_JUNK |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   0.0, 0.0, nullptr, nullptr};
	bool expectValue = true;
	bool needCloseBracket = false;
	bool lastOperationPlusOrMinus = false;
	ArithmeticOpType op = OpPlus;
	const auto skipSpaces = [&it, end]() {
		while (it != end && isspace(*it)) ++it;
	};
	skipSpaces();
	while (it < end) {
		if (expectValue) {
			bool negative = false;
			while (*it == '-' || *it == '+') {
				if (*it == '-') {
					if (lastOperationPlusOrMinus) {
						op = (op == OpPlus) ? OpMinus : OpPlus;
					} else {
						negative = !negative;
					}
				}
				++it;
				skipSpaces();
				if (it == end) throwParseError(fullExpr, it, "The expression unexpected ends after unary operator.");
			}
			if (*it == '(') {
				++it;
				OpenBracket({op, negative});
				it = parse(it, end, containIndexOrFunction, fullExpr, joinedSelectors);
				if (it == end || *it != ')') throwParseError(fullExpr, it, "Expected ')'.");
				++it;
				CloseBracket();
			} else {
				int countOfCharsParsedAsDouble = 0;
				const double value = converter.StringToDouble(it, end - it, &countOfCharsParsedAsDouble);
				if (countOfCharsParsedAsDouble != 0) {
					Append({op, false}, Value{negative ? -value : value});
					it += countOfCharsParsedAsDouble;
				} else {
					const auto parsedIndexName = parseIndexName(it, end, joinedSelectors, fullExpr);
					if (parsedIndexName.joinedSelectorIt == joinedSelectors.cend()) {
						skipSpaces();
						if (it != end && *it == '(') {
							++it;
							const auto funcName = toLower(parsedIndexName.name);
							if (funcName == "rank") {
								Append({op, negative}, Rank{});
								skipSpaces();
							} else if (funcName == "abs") {
								OpenBracket({op, negative}, true);
								it = parse(it, end, containIndexOrFunction, fullExpr, joinedSelectors);
								CloseBracket();
							} else if (funcName == "st_distance") {
								parseDistance(it, end, joinedSelectors, fullExpr, op, negative, skipSpaces);
							} else {
								throwParseError(fullExpr, it, "Unsupported function name : '" + funcName + "'.");
							}
							if (it == end || *it != ')') throwParseError(fullExpr, it, "Expected ')'.");
							++it;
						} else {
							Append({op, negative}, SortExprFuncs::Index{parsedIndexName.name});
						}
					} else {
						Append({op, negative}, JoinedIndex{static_cast<size_t>(parsedIndexName.joinedSelectorIt - joinedSelectors.cbegin()),
														   parsedIndexName.name});
					}
					*containIndexOrFunction = true;
				}
			}
			expectValue = false;
		} else {
			switch (*it) {
				case ')':
					if (needCloseBracket) CloseBracket();
					return it;
				case '+':
				case '-':
					op = (*it == '+') ? OpPlus : OpMinus;
					if (needCloseBracket) {
						CloseBracket();
						needCloseBracket = false;
					}
					lastOperationPlusOrMinus = true;
					break;
				case '*':
				case '/':
					op = (*it == '*') ? OpMult : OpDiv;
					if (lastOperationPlusOrMinus) {
						openBracketBeforeLastAppended();
						needCloseBracket = true;
						lastOperationPlusOrMinus = false;
					}
					break;
				default:
					throwParseError(fullExpr, it, string("Expected ')', '+', '-', '*' of '/', but obtained '") + *it + "'.");
			}
			++it;
			expectValue = true;
		}
		skipSpaces();
	}
	if (expectValue) throwParseError(fullExpr, it, "Expected value.");
	if (needCloseBracket) CloseBracket();
	return it;
}

template <typename T>
SortExpression SortExpression::Parse(string_view expression, const std::vector<T>& joinedSelector) {
	SortExpression result;
	bool containIndexOrFunction = false;
	const auto it = result.parse(expression.begin(), expression.end(), &containIndexOrFunction, expression, joinedSelector);
	if (it != expression.end()) throwParseError(expression, it, "");
	if (!containIndexOrFunction) throwParseError(expression, it, "Expression is undependent on namespace data.");
	return result;
}

template SortExpression SortExpression::Parse(string_view, const std::vector<JoinedSelector>&);
template SortExpression SortExpression::Parse(string_view, const std::vector<JoinedSelectorMock>&);

double SortExpression::calculate(const_iterator it, const_iterator end, IdType rowId, ConstPayload pv,
								 const joins::NamespaceResults& joinedResults, const std::vector<JoinedSelector>& js, uint8_t proc,
								 TagsMatcher& tagsMatcher) {
	assert(it != end);
	assert(it->operation.op == OpPlus);
	double result = 0.0;
	for (; it != end; ++it) {
		double value = it->CalculateAppropriate<double>(
			[&pv, &tagsMatcher, it, proc, rowId, &joinedResults, &js](const SortExpressionBracket& b) {
				const double res = calculate(it.cbegin(), it.cend(), rowId, pv, joinedResults, js, proc, tagsMatcher);
				return (b.IsAbs() && res < 0) ? -res : res;
			},
			[](const Value& v) { return v.value; },
			[&pv, &tagsMatcher](const SortExprFuncs::Index& i) { return i.GetValue(pv, tagsMatcher); },
			[rowId, &joinedResults, &js](const JoinedIndex& i) { return i.GetValue(rowId, joinedResults, js); },
			[proc](const Rank&) -> double { return proc; },
			[&pv, &tagsMatcher](const DistanceFromPoint& i) { return i.GetValue(pv, tagsMatcher); },
			[rowId, &joinedResults, &js](const DistanceJoinedIndexFromPoint& i) { return i.GetValue(rowId, joinedResults, js); },
			[&pv, &tagsMatcher](const DistanceBetweenIndexes& i) { return i.GetValue(pv, tagsMatcher); },
			[&](const DistanceBetweenIndexAndJoinedIndex& i) { return i.GetValue(pv, tagsMatcher, rowId, joinedResults, js); },
			[&](const DistanceBetweenJoinedIndexes& i) { return i.GetValue(rowId, joinedResults, js); },
			[&](const DistanceBetweenJoinedIndexesSameNs& i) { return i.GetValue(rowId, joinedResults, js); });
		if (it->operation.negative) value = -value;
		switch (it->operation.op) {
			case OpPlus:
				result += value;
				break;
			case OpMinus:
				result -= value;
				break;
			case OpMult:
				result *= value;
				break;
			case OpDiv:
				if (value == 0.0) throw Error(errQueryExec, "Division by zero in sort expression");
				result /= value;
				break;
		}
	}
	return result;
}

std::string SortExpression::Dump() const {
	WrSerializer ser;
	dump(cbegin(), cend(), ser);
	return std::string{ser.Slice()};
}

void SortExpression::dump(const_iterator begin, const_iterator end, WrSerializer& ser) {
	assert(begin->operation.op == OpPlus);
	for (const_iterator it = begin; it != end; ++it) {
		if (it != begin) {
			ser << ' ';
			switch (it->operation.op) {
				case OpPlus:
					ser << '+';
					break;
				case OpMinus:
					ser << '-';
					break;
				case OpMult:
					ser << '*';
					break;
				case OpDiv:
					ser << '/';
					break;
			}
			ser << ' ';
		}
		if (it->operation.negative) ser << "(-";
		it->CalculateAppropriate<void>(
			[&it, &ser](const SortExpressionBracket& b) {
				ser << (b.IsAbs() ? "ABS(" : "(");
				dump(it.cbegin(), it.cend(), ser);
				ser << ')';
			},
			[&ser](const Value& v) { ser << v.value; }, [&ser](const SortExprFuncs::Index& i) { ser << i.column; },
			[&ser](const JoinedIndex& i) { ser << "joined " << i.nsIdx << ' ' << i.column; }, [&ser](const Rank&) { ser << "rank()"; },
			[&ser](const DistanceFromPoint& i) { ser << "ST_Distance(" << i.column << ", [" << i.point.x << ", " << i.point.y << "])"; },
			[&ser](const DistanceJoinedIndexFromPoint& i) {
				ser << "ST_Distance(joined " << i.nsIdx << ' ' << i.column << ", [" << i.point.x << ", " << i.point.y << "])";
			},
			[&ser](const DistanceBetweenIndexes& i) { ser << "ST_Distance(" << i.column1 << ", " << i.column2 << ')'; },
			[&ser](const DistanceBetweenIndexAndJoinedIndex& i) {
				ser << "ST_Distance(" << i.column << ", joined " << i.jNsIdx << ' ' << i.jColumn << ')';
			},
			[&ser](const DistanceBetweenJoinedIndexes& i) {
				ser << "ST_Distance(joined " << i.nsIdx1 << ' ' << i.column1 << ", joined " << i.nsIdx2 << ' ' << i.column2 << ')';
			},
			[&ser](const DistanceBetweenJoinedIndexesSameNs& i) {
				ser << "ST_Distance(joined " << i.nsIdx << ' ' << i.column1 << ", joined " << i.nsIdx << ' ' << i.column2 << ')';
			});
		if (it->operation.negative) ser << ')';
	}
}

std::ostream& operator<<(std::ostream& os, const SortExpression& se) { return os << se.Dump(); }

}  // namespace reindexer
