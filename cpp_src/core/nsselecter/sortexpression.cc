#include "sortexpression.h"
#include <set>
#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "estl/charset.h"
#include "estl/restricted.h"
#include "joinedselector.h"
#include "joinedselectormock.h"
#include "tools/stringstools.h"
#include "vendor/double-conversion/double-conversion.h"

namespace {

static RX_NO_INLINE void throwParseError(const std::string_view sortExpr, const char* const pos, const std::string_view message) {
	throw reindexer::Error(errParams, "'%s' is not valid sort expression. Parser failed at position %d.%s%s", sortExpr,
						   pos - sortExpr.data(), message.empty() ? "" : " ", message);
}

static inline double distance(reindexer::Point p1, reindexer::Point p2) noexcept {
	return std::sqrt((p1.X() - p2.X()) * (p1.X() - p2.X()) + (p1.Y() - p2.Y()) * (p1.Y() - p2.Y()));
}

static reindexer::VariantArray getFieldValues(reindexer::ConstPayload pv, reindexer::TagsMatcher& tagsMatcher, int index,
											  std::string_view column) {
	reindexer::VariantArray values;
	if (index == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(column, tagsMatcher, values, reindexer::KeyValueType::Undefined{});
	} else {
		if (index >= pv.NumFields()) {
			throw reindexer::Error(errQueryExec, "Composite fields in sort expression are not supported");
		}
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

const PayloadValue& SortExpression::getJoinedValue(IdType rowId, const joins::NamespaceResults& joinResults,
												   const std::vector<JoinedSelector>& joinedSelectors, size_t nsIdx) {
	assertrx_throw(joinedSelectors.size() > nsIdx);
	const auto& js = joinedSelectors[nsIdx];
	const joins::ItemIterator jIt{&joinResults, rowId};
	const auto jfIt = jIt.at(nsIdx);
	if (jfIt == jIt.end() || jfIt.ItemsCount() == 0) {
		throw Error(errQueryExec, "Not found value joined from ns %s", js.RightNsName());
	}
	if (jfIt.ItemsCount() > 1) {
		throw Error(errQueryExec, "Found more than 1 value joined from ns %s", js.RightNsName());
	}
	return jfIt[0].Value();
}

VariantArray SortExpression::GetJoinedFieldValues(IdType rowId, const joins::NamespaceResults& joinResults,
												  const std::vector<JoinedSelector>& joinedSelectors, size_t nsIdx, std::string_view column,
												  int index) {
	const auto& js = joinedSelectors[nsIdx];
	std::reference_wrapper<const PayloadType> pt = std::visit(
		overloaded{
			[](const JoinPreResult::Values& values) noexcept { return std::cref(values.payloadType); },
			Restricted<IdSet, SelectIteratorContainer>{}([&js](const auto&) noexcept { return std::cref(js.rightNs_->payloadType_); })},
		js.PreResult().payload);
	const ConstPayload pv{pt, getJoinedValue(rowId, joinResults, joinedSelectors, nsIdx)};
	VariantArray values;
	if (index == IndexValueType::SetByJsonPath) {
		TagsMatcher tm = std::visit(overloaded{[](const JoinPreResult::Values& values) noexcept { return std::cref(values.tagsMatcher); },
											   Restricted<IdSet, SelectIteratorContainer>{}(
												   [&js](const auto&) noexcept { return std::cref(js.rightNs_->tagsMatcher_); })},
									js.PreResult().payload);
		pv.GetByJsonPath(column, tm, values, KeyValueType::Undefined{});
	} else {
		pv.Get(index, values);
	}
	return values;
}

bool SortExpression::ByIndexField() const noexcept {
	static constexpr SortExpressionOperation noOperation;
	return Size() == 1 && container_[0].Is<SortExprFuncs::Index>() && GetOperation(0) == noOperation;
}

bool SortExpression::ByJoinedIndexField() const noexcept {
	static constexpr SortExpressionOperation noOperation;
	return Size() == 1 && container_[0].Is<JoinedIndex>() && GetOperation(0) == noOperation;
}

const SortExprFuncs::JoinedIndex& SortExpression::GetJoinedIndex() const noexcept {
	assertrx_throw(Size() == 1);
	return container_[0].Value<JoinedIndex>();
}

double SortExprFuncs::Index::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values = getFieldValues(pv, tagsMatcher, index, column);
	if (values.empty()) {
		throw Error(errQueryExec, "Empty field in sort expression: %s", column);
	}
	if (values.size() > 1 || values[0].Type().Is<KeyValueType::Composite>() || values[0].Type().Is<KeyValueType::Tuple>()) {
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
	const VariantArray values = SortExpression::GetJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx, column, index);
	if (values.empty()) {
		throw Error(errQueryExec, "Empty field in sort expression: %s %s", joinedSelectors[nsIdx].RightNsName(), column);
	}
	if (values.size() > 1 || values[0].Type().Is<KeyValueType::Composite>() || values[0].Type().Is<KeyValueType::Tuple>()) {
		throw Error(errQueryExec, "Array, composite or tuple field in sort expression");
	}
	return values[0].As<double>();
}

double DistanceJoinedIndexFromPoint::GetValue(IdType rowId, const joins::NamespaceResults& joinResults,
											  const std::vector<JoinedSelector>& joinedSelectors) const {
	const VariantArray values = SortExpression::GetJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx, column, index);
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
	const VariantArray values2 = SortExpression::GetJoinedFieldValues(rowId, joinResults, joinedSelectors, jNsIdx, jColumn, jIndex);
	return distance(static_cast<Point>(values1), static_cast<Point>(values2));
}

double DistanceBetweenJoinedIndexes::GetValue(IdType rowId, const joins::NamespaceResults& joinResults,
											  const std::vector<JoinedSelector>& joinedSelectors) const {
	const VariantArray values1 = SortExpression::GetJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx1, column1, index1);
	const VariantArray values2 = SortExpression::GetJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx2, column2, index2);
	return distance(static_cast<Point>(values1), static_cast<Point>(values2));
}

double DistanceBetweenJoinedIndexesSameNs::GetValue(IdType rowId, const joins::NamespaceResults& joinResults,
													const std::vector<JoinedSelector>& joinedSelectors) const {
	const auto& js = joinedSelectors[nsIdx];
	std::reference_wrapper<const PayloadType> pt = std::visit(
		overloaded{
			[](const JoinPreResult::Values& values) noexcept { return std::cref(values.payloadType); },
			Restricted<IdSet, SelectIteratorContainer>{}([&js](const auto&) noexcept { return std::cref(js.rightNs_->payloadType_); })},
		js.PreResult().payload);
	const ConstPayload pv{pt, SortExpression::getJoinedValue(rowId, joinResults, joinedSelectors, nsIdx)};
	TagsMatcher tm = std::visit(overloaded{[](const JoinPreResult::Values& values) noexcept { return std::cref(values.tagsMatcher); },
										   Restricted<IdSet, SelectIteratorContainer>{}(
											   [&js](const auto&) noexcept { return std::cref(js.rightNs_->tagsMatcher_); })},
								js.PreResult().payload);
	VariantArray values1;
	if (index1 == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(column1, tm, values1, KeyValueType::Undefined{});
	} else {
		pv.Get(index1, values1);
	}
	VariantArray values2;
	if (index2 == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(column2, tm, values2, KeyValueType::Undefined{});
	} else {
		pv.Get(index2, values2);
	}
	return distance(static_cast<Point>(values1), static_cast<Point>(values2));
}

void SortExpression::openBracketBeforeLastAppended() {
	const size_t pos = LastAppendedElement();
	assertrx_throw(activeBrackets_.empty() || activeBrackets_.back() < pos);
	for (unsigned i : activeBrackets_) {
		assertrx_throw(i < container_.size());
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
	std::string_view name;
};

constexpr static estl::Charset kIndexNameSyms{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
											  'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
											  'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
											  'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', '+', '"'};

template <typename T>
static ParseIndexNameResult<T> parseIndexName(std::string_view& expr, const std::vector<T>& joinedSelectors,
											  const std::string_view fullExpr) {
	auto pos = expr.data();
	const auto end = expr.data() + expr.size();
	auto joinedSelectorIt = joinedSelectors.cend();
	bool joinedFieldInQuotes = false;
	while (pos != end && *pos != '.' && kIndexNameSyms.test(*pos)) {
		++pos;
	}
	if (pos != end && *pos == '.') {
		std::string_view namespaceName = {expr.data(), static_cast<size_t>(pos - expr.data())};

		// Check for quotes in join expression to skip them
		joinedFieldInQuotes = namespaceName.at(0) == '"';
		if (joinedFieldInQuotes) {
			namespaceName.remove_prefix(1);
		}

		++pos;
		joinedSelectorIt = std::find_if(joinedSelectors.cbegin(), joinedSelectors.cend(),
										[namespaceName](const T& js) { return namespaceName == js.RightNsName(); });
		if (joinedSelectorIt != joinedSelectors.cend()) {
			if (std::find_if(joinedSelectorIt + 1, joinedSelectors.cend(),
							 [namespaceName](const T& js) { return namespaceName == js.RightNsName(); }) != joinedSelectors.cend()) {
				throwParseError(fullExpr, pos,
								"Sorting by namespace which has been joined more than once: '" + std::string(namespaceName) + "'.");
			}
			expr.remove_prefix(pos - expr.data());
		} else {
			joinedFieldInQuotes = false;
		}
	}
	while (pos != end && kIndexNameSyms.test(*pos)) {
		++pos;
	}
	std::string_view name{expr.data(), static_cast<size_t>(pos - expr.data())};
	if (name.empty()) {
		throwParseError(fullExpr, pos, "Expected index or function name.");
	}

	if (joinedFieldInQuotes) {	// Namespace in quotes - trim closing quote
		if (name.back() != '"') {
			throwParseError(fullExpr, pos, "Closing quote not found");
		}
		name.remove_suffix(1);
	} else if (name[0] == '"') {  // In case without join
		name.remove_prefix(1);
		if (name.back() != '"') {
			throwParseError(fullExpr, pos, "Closing quote not found");
		}
		name.remove_suffix(1);
	}

	expr.remove_prefix(pos - expr.data());
	return {joinedSelectorIt, name};
}

template <typename SkipWS>
static Point parsePoint(std::string_view& expr, std::string_view funcName, const std::string_view fullExpr, const SkipWS& skipSpaces) {
	using namespace double_conversion;
	using namespace std::string_view_literals;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_TRAILING_JUNK |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   0.0, 0.0, nullptr, nullptr};
	if (funcName != "st_geomfromtext") {
		throwParseError(fullExpr, expr.data(), "Unsupported function inside ST_Distance() : '" + std::string(funcName) + "'.");
	}
	expr.remove_prefix(1);
	skipSpaces();
	if (expr.empty() || (expr[0] != '\'' && expr[0] != '"')) {
		throwParseError(fullExpr, expr.data(), "Expected \" or '.");
	}
	const char openQuote = expr[0];
	expr.remove_prefix(1);
	skipSpaces();
	if (!checkIfStartsWith("point"sv, expr)) {
		throwParseError(fullExpr, expr.data(), "Expected 'point'.");
	}
	expr.remove_prefix(5);
	skipSpaces();
	if (expr.empty() || expr[0] != '(') {
		throwParseError(fullExpr, expr.data(), "Expected '('.");
	}
	expr.remove_prefix(1);
	skipSpaces();
	int countOfCharsParsedAsDouble = 0;
	const double x = converter.StringToDouble(expr.data(), expr.size(), &countOfCharsParsedAsDouble);
	if (countOfCharsParsedAsDouble == 0) {
		throwParseError(fullExpr, expr.data(), "Expected number.");
	}
	expr.remove_prefix(countOfCharsParsedAsDouble);
	skipSpaces();
	countOfCharsParsedAsDouble = 0;
	const double y = converter.StringToDouble(expr.data(), expr.size(), &countOfCharsParsedAsDouble);
	if (countOfCharsParsedAsDouble == 0) {
		throwParseError(fullExpr, expr.data(), "Expected number.");
	}
	expr.remove_prefix(countOfCharsParsedAsDouble);
	skipSpaces();
	if (expr.empty() || expr[0] != ')') {
		throwParseError(fullExpr, expr.data(), "Expected ')'.");
	}
	expr.remove_prefix(1);
	skipSpaces();
	if (expr.empty() || expr[0] != openQuote) {
		throwParseError(fullExpr, expr.data(), std::string("Expected ") + openQuote + '.');
	}
	expr.remove_prefix(1);
	skipSpaces();
	if (expr.empty() || expr[0] != ')') {
		throwParseError(fullExpr, expr.data(), "Expected ')'.");
	}
	expr.remove_prefix(1);
	return Point{x, y};
}

template <typename T, typename SkipSW>
void SortExpression::parseDistance(std::string_view& expr, const std::vector<T>& joinedSelectors, const std::string_view fullExpr,
								   const ArithmeticOpType op, const bool negative, const SkipSW& skipSpaces) {
	skipSpaces();
	const auto parsedIndexName1 = parseIndexName(expr, joinedSelectors, fullExpr);
	skipSpaces();
	if (parsedIndexName1.joinedSelectorIt != joinedSelectors.cend()) {
		if (expr.empty() || expr[0] != ',') {
			throwParseError(fullExpr, expr.data(), "Expected ','.");
		}
		expr.remove_prefix(1);
		skipSpaces();
		const size_t jNsIdx1 = static_cast<size_t>(parsedIndexName1.joinedSelectorIt - joinedSelectors.cbegin());
		const auto parsedIndexName2 = parseIndexName(expr, joinedSelectors, fullExpr);
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			if (parsedIndexName1.joinedSelectorIt == parsedIndexName2.joinedSelectorIt) {
				if (toLower(parsedIndexName1.name) == toLower(parsedIndexName2.name)) {
					throwParseError(fullExpr, expr.data(), "Distance between two same indexes");
				}
				Append({op, negative}, DistanceBetweenJoinedIndexesSameNs{jNsIdx1, parsedIndexName1.name, parsedIndexName2.name});
			} else {
				Append({op, negative},
					   DistanceBetweenJoinedIndexes{jNsIdx1, parsedIndexName1.name,
													static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
													parsedIndexName2.name});
			}
		} else {
			skipSpaces();
			if (!expr.empty() && expr[0] == '(') {
				const auto point = parsePoint(expr, toLower(parsedIndexName2.name), fullExpr, skipSpaces);
				Append({op, negative}, DistanceJoinedIndexFromPoint{jNsIdx1, parsedIndexName1.name, point});
			} else {
				Append({op, negative}, DistanceBetweenIndexAndJoinedIndex{parsedIndexName2.name, jNsIdx1, parsedIndexName1.name});
			}
		}
	} else if (!expr.empty() && expr[0] == '(') {
		const auto point = parsePoint(expr, toLower(parsedIndexName1.name), fullExpr, skipSpaces);
		skipSpaces();
		if (expr.empty() || expr[0] != ',') {
			throwParseError(fullExpr, expr.data(), "Expected ','.");
		}
		expr.remove_prefix(1);
		skipSpaces();
		const auto parsedIndexName2 = parseIndexName(expr, joinedSelectors, fullExpr);
		skipSpaces();
		if (!expr.empty() && expr[0] == '(') {
			throwParseError(fullExpr, expr.data(), "Allowed only one function inside ST_Geometry");
		}
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			Append({op, negative},
				   DistanceJoinedIndexFromPoint{static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
												parsedIndexName2.name, point});
		} else {
			Append({op, negative}, DistanceFromPoint{parsedIndexName2.name, point});
		}
	} else {
		if (expr.empty() || expr[0] != ',') {
			throwParseError(fullExpr, expr.data(), "Expected ','.");
		}
		expr.remove_prefix(1);
		skipSpaces();
		const auto parsedIndexName2 = parseIndexName(expr, joinedSelectors, fullExpr);
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			Append({op, negative},
				   DistanceBetweenIndexAndJoinedIndex{parsedIndexName1.name,
													  static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
													  parsedIndexName2.name});
		} else {
			skipSpaces();
			if (!expr.empty() && expr[0] == '(') {
				const auto point = parsePoint(expr, toLower(parsedIndexName2.name), fullExpr, skipSpaces);
				Append({op, negative}, DistanceFromPoint{parsedIndexName1.name, point});
			} else {
				if (toLower(parsedIndexName1.name) == toLower(parsedIndexName2.name)) {
					throwParseError(fullExpr, expr.data(), "Distance between two same indexes");
				}
				Append({op, negative}, DistanceBetweenIndexes{parsedIndexName1.name, parsedIndexName2.name});
			}
		}
	}
	skipSpaces();
}

template <typename T>
std::string_view SortExpression::parse(std::string_view expr, bool* containIndexOrFunction, const std::string_view fullExpr,
									   const std::vector<T>& joinedSelectors) {
	using namespace double_conversion;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_TRAILING_JUNK |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   0.0, 0.0, nullptr, nullptr};
	bool expectValue = true;
	bool needCloseBracket = false;
	bool lastOperationPlusOrMinus = false;
	ArithmeticOpType op = OpPlus;
	const auto skipSpaces = [&expr]() {
		while (!expr.empty() && isspace(expr[0])) {
			expr.remove_prefix(1);
		}
	};
	skipSpaces();
	while (!expr.empty()) {
		if (expectValue) {
			bool negative = false;
			while (expr[0] == '-' || expr[0] == '+') {
				if (expr[0] == '-') {
					if (lastOperationPlusOrMinus) {
						op = (op == OpPlus) ? OpMinus : OpPlus;
					} else {
						negative = !negative;
					}
				}
				expr.remove_prefix(1);
				skipSpaces();
				if (expr.empty()) {
					throwParseError(fullExpr, expr.data(), "The expression unexpected ends after unary operator.");
				}
			}
			if (expr[0] == '(') {
				expr.remove_prefix(1);
				OpenBracket({op, negative});
				expr = parse(expr, containIndexOrFunction, fullExpr, joinedSelectors);
				if (expr.empty() || expr[0] != ')') {
					throwParseError(fullExpr, expr.data(), "Expected ')'.");
				}
				expr.remove_prefix(1);
				CloseBracket();
			} else if (expr[0] == '"') {
				const auto parsedIndexName = parseIndexName(expr, joinedSelectors, fullExpr);
				if (parsedIndexName.joinedSelectorIt == joinedSelectors.cend()) {
					skipSpaces();
					Append({op, negative}, SortExprFuncs::Index{parsedIndexName.name});
				} else {
					auto dist = static_cast<size_t>(parsedIndexName.joinedSelectorIt - joinedSelectors.cbegin());
					Append({op, negative}, JoinedIndex{dist, parsedIndexName.name});
				}
				*containIndexOrFunction = true;
			} else {
				int countOfCharsParsedAsDouble = 0;
				const double value = converter.StringToDouble(expr.data(), expr.size(), &countOfCharsParsedAsDouble);
				if (countOfCharsParsedAsDouble != 0) {
					Append({op, false}, Value{negative ? -value : value});
					expr.remove_prefix(countOfCharsParsedAsDouble);
				} else {
					const auto parsedIndexName = parseIndexName(expr, joinedSelectors, fullExpr);
					if (parsedIndexName.joinedSelectorIt == joinedSelectors.cend()) {
						skipSpaces();
						if (!expr.empty() && expr[0] == '(') {
							expr.remove_prefix(1);
							const auto funcName = toLower(parsedIndexName.name);
							if (funcName == "rank") {
								Append({op, negative}, Rank{});
								skipSpaces();
							} else if (funcName == "abs") {
								OpenBracket({op, negative}, true);
								expr = parse(expr, containIndexOrFunction, fullExpr, joinedSelectors);
								CloseBracket();
							} else if (funcName == "st_distance") {
								parseDistance(expr, joinedSelectors, fullExpr, op, negative, skipSpaces);
							} else {
								throwParseError(fullExpr, expr.data(), "Unsupported function name : '" + funcName + "'.");
							}
							if (expr.empty() || expr[0] != ')') {
								throwParseError(fullExpr, expr.data(), "Expected ')'.");
							}
							expr.remove_prefix(1);
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
			switch (expr[0]) {
				case ')':
					if (needCloseBracket) {
						CloseBracket();
					}
					return expr;
				case '+':
				case '-':
					op = (expr[0] == '+') ? OpPlus : OpMinus;
					if (needCloseBracket) {
						CloseBracket();
						needCloseBracket = false;
					}
					lastOperationPlusOrMinus = true;
					break;
				case '*':
				case '/':
					op = (expr[0] == '*') ? OpMult : OpDiv;
					if (lastOperationPlusOrMinus) {
						openBracketBeforeLastAppended();
						needCloseBracket = true;
						lastOperationPlusOrMinus = false;
					}
					break;
				default:
					throwParseError(fullExpr, expr.data(),
									std::string("Expected ')', '+', '-', '*' of '/', but obtained '") + expr[0] + "'.");
			}
			expr.remove_prefix(1);
			expectValue = true;
		}
		skipSpaces();
	}
	if (expectValue) {
		throwParseError(fullExpr, expr.data(), "Expected value.");
	}
	if (needCloseBracket) {
		CloseBracket();
	}
	return expr;
}

template <typename T>
SortExpression SortExpression::Parse(const std::string_view expression, const std::vector<T>& joinedSelector) {
	SortExpression result;
	bool containIndexOrFunction = false;
	const auto expr = result.parse(expression, &containIndexOrFunction, expression, joinedSelector);
	if (!expr.empty()) {
		throwParseError(expression, expr.data(), "");
	}
	if (!containIndexOrFunction) {
		throwParseError(expression, expr.data(), "Sort expression does not depend from namespace data");
	}
	return result;
}

template SortExpression SortExpression::Parse(std::string_view, const std::vector<JoinedSelector>&);
template SortExpression SortExpression::Parse(std::string_view, const std::vector<JoinedSelectorMock>&);
template SortExpression SortExpression::Parse(std::string_view, const std::vector<JoinedNsNameMock>&);

double SortExpression::calculate(const_iterator it, const_iterator end, IdType rowId, ConstPayload pv,
								 const joins::NamespaceResults* joinedResults, const std::vector<JoinedSelector>& js, uint8_t proc,
								 TagsMatcher& tagsMatcher) {
	assertrx_throw(it != end);
	assertrx_throw(it->operation.op == OpPlus);
	double result = 0.0;
	for (; it != end; ++it) {
		double value = it->Visit(
			[&pv, &tagsMatcher, it, proc, rowId, joinedResults, &js] RX_PRE_LMBD_ALWAYS_INLINE(const SortExpressionBracket& b)
				RX_POST_LMBD_ALWAYS_INLINE {
					const double res = calculate(it.cbegin(), it.cend(), rowId, pv, joinedResults, js, proc, tagsMatcher);
					return (b.IsAbs() && res < 0) ? -res : res;
				},
			[] RX_PRE_LMBD_ALWAYS_INLINE(const Value& v) RX_POST_LMBD_ALWAYS_INLINE { return v.value; },
			[&pv, &tagsMatcher] RX_PRE_LMBD_ALWAYS_INLINE(const SortExprFuncs::Index& i)
				RX_POST_LMBD_ALWAYS_INLINE { return i.GetValue(pv, tagsMatcher); },
			[rowId, joinedResults, &js] RX_PRE_LMBD_ALWAYS_INLINE(const JoinedIndex& i) RX_POST_LMBD_ALWAYS_INLINE {
				assertrx_throw(joinedResults);
				return i.GetValue(rowId, *joinedResults, js);
			},
			[proc] RX_PRE_LMBD_ALWAYS_INLINE(const Rank&) RX_POST_LMBD_ALWAYS_INLINE -> double { return proc; },
			[&pv, &tagsMatcher] RX_PRE_LMBD_ALWAYS_INLINE(const DistanceFromPoint& i)
				RX_POST_LMBD_ALWAYS_INLINE { return i.GetValue(pv, tagsMatcher); },
			[rowId, joinedResults, &js] RX_PRE_LMBD_ALWAYS_INLINE(const DistanceJoinedIndexFromPoint& i) RX_POST_LMBD_ALWAYS_INLINE {
				assertrx_throw(joinedResults);
				return i.GetValue(rowId, *joinedResults, js);
			},
			[&pv, &tagsMatcher] RX_PRE_LMBD_ALWAYS_INLINE(const DistanceBetweenIndexes& i)
				RX_POST_LMBD_ALWAYS_INLINE { return i.GetValue(pv, tagsMatcher); },
			[&] RX_PRE_LMBD_ALWAYS_INLINE(const DistanceBetweenIndexAndJoinedIndex& i) RX_POST_LMBD_ALWAYS_INLINE {
				assertrx_throw(joinedResults);
				return i.GetValue(pv, tagsMatcher, rowId, *joinedResults, js);
			},
			[&] RX_PRE_LMBD_ALWAYS_INLINE(const DistanceBetweenJoinedIndexes& i) RX_POST_LMBD_ALWAYS_INLINE {
				assertrx_throw(joinedResults);
				return i.GetValue(rowId, *joinedResults, js);
			},
			[&] RX_PRE_LMBD_ALWAYS_INLINE(const DistanceBetweenJoinedIndexesSameNs& i) RX_POST_LMBD_ALWAYS_INLINE {
				assertrx_throw(joinedResults);
				return i.GetValue(rowId, *joinedResults, js);
			});
		if (it->operation.negative) {
			value = -value;
		}
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
				if (value == 0.0) {
					throw Error(errQueryExec, "Division by zero in sort expression");
				}
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
	assertrx_throw(begin->operation.op == OpPlus);
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
		if (it->operation.negative) {
			ser << "(-";
		}
		it->Visit(
			[&it, &ser](const SortExpressionBracket& b) {
				ser << (b.IsAbs() ? "ABS(" : "(");
				dump(it.cbegin(), it.cend(), ser);
				ser << ')';
			},
			[&ser](const Value& v) { ser << v.value; }, [&ser](const SortExprFuncs::Index& i) { ser << i.column; },
			[&ser](const JoinedIndex& i) { ser << "joined " << i.nsIdx << ' ' << i.column; }, [&ser](const Rank&) { ser << "rank()"; },
			[&ser](const DistanceFromPoint& i) {
				ser << "ST_Distance(" << i.column << ", [" << i.point.X() << ", " << i.point.Y() << "])";
			},
			[&ser](const DistanceJoinedIndexFromPoint& i) {
				ser << "ST_Distance(joined " << i.nsIdx << ' ' << i.column << ", [" << i.point.X() << ", " << i.point.Y() << "])";
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
		if (it->operation.negative) {
			ser << ')';
		}
	}
}

std::ostream& operator<<(std::ostream& os, const SortExpression& se) { return os << se.Dump(); }

}  // namespace reindexer
