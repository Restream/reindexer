#include "sortexpression.h"
#include <charconv>
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/joinedselectormock.h"
#include "core/queryresults/joinresults.h"
#include "estl/charset.h"
#include "reranker.h"
#include "tools/stringstools.h"
#include "vendor/double-conversion/double-conversion.h"
#include "vendor/murmurhash/MurmurHash3.h"

namespace {

RX_NO_INLINE void throwParseError(std::string_view sortExpr, int pos, std::string_view message) {
	throw reindexer::Error(errParams, "'{}' is not valid sort expression. Parser failed at position {}.{}{}", sortExpr, pos,
						   message.empty() ? "" : " ", message);
}

inline double distance(reindexer::Point p1, reindexer::Point p2) noexcept {
	return std::sqrt((p1.X() - p2.X()) * (p1.X() - p2.X()) + (p1.Y() - p2.Y()) * (p1.Y() - p2.Y()));
}

reindexer::VariantArray getFieldValues(reindexer::ConstPayload pv, reindexer::TagsMatcher& tagsMatcher, int index,
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

reindexer::VariantArray getJsonFieldValues(reindexer::ConstPayload pv, reindexer::TagsMatcher& tagsMatcher, std::string_view json) {
	reindexer::VariantArray values;
	pv.GetByJsonPath(json, tagsMatcher, values, reindexer::KeyValueType::Undefined{});
	return values;
}

}  // namespace

namespace reindexer {

using namespace SortExprFuncs;
using namespace std::string_view_literals;

const PayloadValue& SortExpression::getJoinedValue(IdType rowId, const joins::NamespaceResults& joinResults,
												   const std::vector<JoinedSelector>& joinedSelectors, size_t nsIdx) {
	assertrx_throw(joinedSelectors.size() > nsIdx);
	const auto& js = joinedSelectors[nsIdx];
	const joins::ItemIterator jIt{&joinResults, rowId};
	const auto jfIt = jIt.at(nsIdx);
	if (jfIt == jIt.end() || jfIt.ItemsCount() == 0) {
		throw Error(errQueryExec, "Not found value joined from ns {}", js.RightNsName());
	}
	if (jfIt.ItemsCount() > 1) {
		throw Error(errQueryExec, "Found more than 1 value joined from ns {}", js.RightNsName());
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
			[&js]<concepts::OneOf<IdSet, SelectIteratorContainer> T>(const T&) noexcept { return std::cref(js.rightNs_->payloadType_); }},
		js.PreResult().payload);
	const ConstPayload pv{pt, getJoinedValue(rowId, joinResults, joinedSelectors, nsIdx)};
	VariantArray values;
	if (index == IndexValueType::SetByJsonPath) {
		TagsMatcher tm = std::visit(overloaded{[](const JoinPreResult::Values& values) noexcept { return std::cref(values.tagsMatcher); },
											   [&js]<concepts::OneOf<IdSet, SelectIteratorContainer> T>(const T&) noexcept {
												   return std::cref(js.rightNs_->tagsMatcher_);
											   }},
									js.PreResult().payload);
		pv.GetByJsonPath(column, tm, values, KeyValueType::Undefined{});
	} else {
		pv.Get(index, values);
	}
	return values;
}

bool SortExpression::ByField() const noexcept {
	static constexpr SortExpressionOperation noOperation;
	return Size() == 1 && container_[0].Is<SortExprFuncs::Index>() && GetOperation(0) == noOperation;
}

bool SortExpression::ByJoinedField() const noexcept {
	static constexpr SortExpressionOperation noOperation;
	return Size() == 1 && container_[0].Is<JoinedIndex>() && GetOperation(0) == noOperation;
}

SortExprFuncs::JoinedIndex& SortExpression::GetJoinedIndex() noexcept {
	assertrx_throw(Size() == 1);
	return container_[0].Value<JoinedIndex>();
}

double SortExprFuncs::Index::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values = getFieldValues(pv, tagsMatcher, index, column);
	if (values.empty()) {
		throw Error(errQueryExec, "Empty field in sort expression: {}", column);
	}
	if (values.size() > 1 || values[0].Type().Is<KeyValueType::Composite>() || values[0].Type().Is<KeyValueType::Tuple>()) {
		throw Error(errQueryExec, "Array, composite or tuple field in sort expression");
	}
	return values[0].As<double>();
}

double SortExprFuncs::ProxiedField::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values = getJsonFieldValues(pv, tagsMatcher, json);
	if (values.empty()) {
		throw Error(errQueryExec, "Empty field in sort expression: {}", json);
	}
	if (values.size() > 1 || values[0].Type().Is<KeyValueType::Composite>() || values[0].Type().Is<KeyValueType::Tuple>()) {
		throw Error(errQueryExec, "Array, composite or tuple field in sort expression are not supported");
	}
	return values[0].As<double>();
}

double DistanceFromPoint::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values = getFieldValues(pv, tagsMatcher, index, column);
	return distance(static_cast<Point>(values), point);
}

double ProxiedDistanceFromPoint::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values = getJsonFieldValues(pv, tagsMatcher, json);
	return distance(static_cast<Point>(values), point);
}

double JoinedIndex::GetValue(IdType rowId, const joins::NamespaceResults& joinResults,
							 const std::vector<JoinedSelector>& joinedSelectors) const {
	const VariantArray values = SortExpression::GetJoinedFieldValues(rowId, joinResults, joinedSelectors, nsIdx, column, index);
	if (values.empty()) {
		throw Error(errQueryExec, "Empty field in sort expression: {} {}", joinedSelectors[nsIdx].RightNsName(), column);
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

double ProxiedDistanceBetweenFields::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	const VariantArray values1 = getJsonFieldValues(pv, tagsMatcher, json1);
	const VariantArray values2 = getJsonFieldValues(pv, tagsMatcher, json2);
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
			[&js]<concepts::OneOf<IdSet, SelectIteratorContainer> T>(const T&) noexcept { return std::cref(js.rightNs_->payloadType_); }},
		js.PreResult().payload);
	const ConstPayload pv{pt, SortExpression::getJoinedValue(rowId, joinResults, joinedSelectors, nsIdx)};
	TagsMatcher tm = std::visit(overloaded{[](const JoinPreResult::Values& values) noexcept { return std::cref(values.tagsMatcher); },
										   [&js]<concepts::OneOf<IdSet, SelectIteratorContainer> T>(const T&) noexcept {
											   return std::cref(js.rightNs_->tagsMatcher_);
										   }},
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

template <typename T>
struct [[nodiscard]] ParseIndexNameResult {
	typename std::vector<T>::const_iterator joinedSelectorIt;
	std::string name;
};

constexpr static estl::Charset kIndexNameSyms{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
											  'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
											  'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
											  'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', '+', '"'};

template <typename T>
static ParseIndexNameResult<T> parseIndexName(std::string_view& expr, const std::vector<T>& joinedSelectors, std::string_view fullExpr) {
	auto pos = expr.data();
	const auto end = expr.data() + expr.size();
	auto joinedSelectorIt = joinedSelectors.cend();
	bool joinedFieldInQuotes = false;
	size_t quotes = 0;
	while (pos != end && *pos != '.' && kIndexNameSyms.test(*pos)) {
		quotes += (*pos == '"');
		++pos;
	}
	if (pos != end && *pos == '.') {
		// NOLINTNEXTLINE (bugprone-suspicious-stringview-data-usage)
		std::string_view namespaceName{expr.data(), static_cast<size_t>(pos - expr.data())};

		// Check for quotes in join expression to skip them
		joinedFieldInQuotes = namespaceName.at(0) == '"';
		if (joinedFieldInQuotes) {
			namespaceName.remove_prefix(1);
		}

		++pos;
		joinedSelectorIt = std::find_if(joinedSelectors.cbegin(), joinedSelectors.cend(),
										[namespaceName](const T& js) { return iequals(namespaceName, js.RightNsName()); });
		if (joinedSelectorIt != joinedSelectors.cend()) {
			if (std::find_if(joinedSelectorIt + 1, joinedSelectors.cend(),
							 [namespaceName](const T& js) { return iequals(namespaceName, js.RightNsName()); }) != joinedSelectors.cend()) {
				throwParseError(fullExpr, pos - fullExpr.data(),
								fmt::format("Sorting by namespace which has been joined more than once: '{}'", namespaceName));
			}
			expr.remove_prefix(pos - expr.data());
		} else {
			joinedFieldInQuotes = false;
		}
	}
	while (pos != end && kIndexNameSyms.test(*pos)) {
		quotes += (*pos == '"');
		++pos;
	}

	// NOLINTNEXTLINE (bugprone-suspicious-stringview-data-usage)
	std::string_view name{expr.data(), static_cast<size_t>(pos - expr.data())};
	if (joinedFieldInQuotes) {	// Namespace in quotes - trim closing quote
		if (name.back() != '"') [[unlikely]] {
			throwParseError(fullExpr, pos - fullExpr.data(), "Closing quote not found");
		}
		if (quotes != 2) [[unlikely]] {
			throwParseError(fullExpr, pos - fullExpr.data(), "Unexpected quotes in the middle of the joined field name");
		}
		name.remove_suffix(1);
	} else if (quotes) {  // In case without join
		assertrx_throw(!name.empty());
		if (name[0] != '"' || quotes > 2) [[unlikely]] {
			throwParseError(fullExpr, pos - fullExpr.data(), "Unexpected quotes in the middle of the field name");
		}
		name.remove_prefix(1);
		if (name.empty() || name.back() != '"') [[unlikely]] {
			throwParseError(fullExpr, pos - fullExpr.data(), "Closing quote not found");
		}
		name.remove_suffix(1);
	}
	if (name.empty()) [[unlikely]] {
		throwParseError(fullExpr, pos - fullExpr.data(), "Expected index or function name");
	}

	expr.remove_prefix(pos - expr.data());
	return {joinedSelectorIt, std::string{name}};
}

template <typename SkipWS>
static Point parsePoint(std::string_view& expr, std::string_view funcName, std::string_view fullExpr, const SkipWS& skipSpaces) {
	using namespace double_conversion;
	using namespace std::string_view_literals;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_TRAILING_JUNK |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   0.0, 0.0, nullptr, nullptr};
	if (funcName != "st_geomfromtext") {
		throwParseError(fullExpr, expr.data() - fullExpr.data(),
						"Unsupported function inside ST_Distance() : '" + std::string(funcName) + "'.");
	}
	expr.remove_prefix(1);
	skipSpaces();
	if (expr.empty() || (expr[0] != '\'' && expr[0] != '"')) {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected \" or '.");
	}
	const char openQuote = expr[0];
	expr.remove_prefix(1);
	skipSpaces();
	if (!checkIfStartsWith("point"sv, expr)) {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected 'point'.");
	}
	expr.remove_prefix(5);
	skipSpaces();
	if (expr.empty() || expr[0] != '(') {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected '('.");
	}
	expr.remove_prefix(1);
	skipSpaces();
	int countOfCharsParsedAsDouble = 0;
	const double x = converter.StringToDouble(expr.data(), expr.size(), &countOfCharsParsedAsDouble);
	if (countOfCharsParsedAsDouble == 0) {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected number.");
	}
	expr.remove_prefix(countOfCharsParsedAsDouble);
	skipSpaces();
	countOfCharsParsedAsDouble = 0;
	const double y = converter.StringToDouble(expr.data(), expr.size(), &countOfCharsParsedAsDouble);
	if (countOfCharsParsedAsDouble == 0) {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected number.");
	}
	expr.remove_prefix(countOfCharsParsedAsDouble);
	skipSpaces();
	if (expr.empty() || expr[0] != ')') {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected ')'.");
	}
	expr.remove_prefix(1);
	skipSpaces();
	if (expr.empty() || expr[0] != openQuote) {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), std::string("Expected ") + openQuote + '.');
	}
	expr.remove_prefix(1);
	skipSpaces();
	if (expr.empty() || expr[0] != ')') {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected ')'.");
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
			throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected ','.");
		}
		expr.remove_prefix(1);
		skipSpaces();
		const size_t jNsIdx1 = static_cast<size_t>(parsedIndexName1.joinedSelectorIt - joinedSelectors.cbegin());
		const auto parsedIndexName2 = parseIndexName(expr, joinedSelectors, fullExpr);
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			if (parsedIndexName1.joinedSelectorIt == parsedIndexName2.joinedSelectorIt) {
				if (iequals(parsedIndexName1.name, parsedIndexName2.name)) {
					throwParseError(fullExpr, expr.data() - fullExpr.data(), "Distance between two identical indexes");
				}
				std::ignore = Append({op, negative}, DistanceBetweenJoinedIndexesSameNs{jNsIdx1, std::move(parsedIndexName1.name),
																						std::move(parsedIndexName2.name)});
			} else {
				std::ignore = Append({op, negative}, DistanceBetweenJoinedIndexes{
														 jNsIdx1, std::move(parsedIndexName1.name),
														 static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
														 std::move(parsedIndexName2.name)});
			}
		} else {
			skipSpaces();
			if (!expr.empty() && expr[0] == '(') {
				const auto point = parsePoint(expr, toLower(parsedIndexName2.name), fullExpr, skipSpaces);
				std::ignore = Append({op, negative}, DistanceJoinedIndexFromPoint{jNsIdx1, std::move(parsedIndexName1.name), point});
			} else {
				std::ignore = Append({op, negative}, DistanceBetweenIndexAndJoinedIndex{std::move(parsedIndexName2.name), jNsIdx1,
																						std::move(parsedIndexName1.name)});
			}
		}
	} else if (!expr.empty() && expr[0] == '(') {
		const auto point = parsePoint(expr, toLower(parsedIndexName1.name), fullExpr, skipSpaces);
		skipSpaces();
		if (expr.empty() || expr[0] != ',') {
			throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected ','.");
		}
		expr.remove_prefix(1);
		skipSpaces();
		const auto parsedIndexName2 = parseIndexName(expr, joinedSelectors, fullExpr);
		skipSpaces();
		if (!expr.empty() && expr[0] == '(') {
			throwParseError(fullExpr, expr.data() - fullExpr.data(), "Allowed only one function inside ST_Geometry");
		}
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			std::ignore = Append({op, negative}, DistanceJoinedIndexFromPoint{
													 static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
													 std::move(parsedIndexName2.name), point});
		} else {
			std::ignore = Append({op, negative}, DistanceFromPoint{std::move(parsedIndexName2.name), point});
		}
	} else {
		if (expr.empty() || expr[0] != ',') {
			throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected ','.");
		}
		expr.remove_prefix(1);
		skipSpaces();
		const auto parsedIndexName2 = parseIndexName(expr, joinedSelectors, fullExpr);
		if (parsedIndexName2.joinedSelectorIt != joinedSelectors.cend()) {
			std::ignore = Append({op, negative}, DistanceBetweenIndexAndJoinedIndex{
													 std::move(parsedIndexName1.name),
													 static_cast<size_t>(parsedIndexName2.joinedSelectorIt - joinedSelectors.cbegin()),
													 std::move(parsedIndexName2.name)});
		} else {
			skipSpaces();
			if (!expr.empty() && expr[0] == '(') {
				const auto point = parsePoint(expr, toLower(parsedIndexName2.name), fullExpr, skipSpaces);
				std::ignore = Append({op, negative}, DistanceFromPoint{std::move(parsedIndexName1.name), point});
			} else {
				if (iequals(parsedIndexName1.name, parsedIndexName2.name)) {
					throwParseError(fullExpr, expr.data() - fullExpr.data(), "Distance between two identical indexes");
				}
				std::ignore =
					Append({op, negative}, DistanceBetweenIndexes{std::move(parsedIndexName1.name), std::move(parsedIndexName2.name)});
			}
		}
	}
	skipSpaces();
}

template <typename T, typename SkipSW>
void SortExpression::parseRank(std::string_view& expr, const std::vector<T>& joinedSelectors, const std::string_view fullExpr,
							   const ArithmeticOpType op, const bool negative, const SkipSW& skipSpaces) {
	using namespace double_conversion;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_TRAILING_JUNK |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   0.0, 0.0, nullptr, nullptr};
	skipSpaces();
	if (!expr.empty() && expr[0] != ')') {
		auto rankIndexName = parseIndexName(expr, joinedSelectors, fullExpr);
		if (rankIndexName.joinedSelectorIt != joinedSelectors.cend()) {
			throwParseError(fullExpr, expr.data() - fullExpr.data(), "Rank by joined field '" + rankIndexName.name + '\'');
		}
		skipSpaces();
		double defaultValue = 0.0;
		if (!expr.empty() && expr[0] == ',') {
			expr.remove_prefix(1);
			skipSpaces();
			int countOfCharsParsedAsDouble = 0;
			defaultValue = converter.StringToDouble(expr.data(), expr.size(), &countOfCharsParsedAsDouble);
			if (countOfCharsParsedAsDouble == 0) {
				throwParseError(fullExpr, expr.data() - fullExpr.data(), "Default value of rank function is expected"sv);
			}
			expr.remove_prefix(countOfCharsParsedAsDouble);
			skipSpaces();
		}
		std::ignore = Append<RankNamed>({op, negative}, std::move(rankIndexName.name), defaultValue);
	} else {
		std::ignore = Append<Rank>({op, negative});
	}
}

template <typename T>
std::string_view SortExpression::parse(std::string_view expr, bool* containIndexOrFunction, bool* isRrf, const std::string_view fullExpr,
									   const std::vector<T>& joinedSelectors) {
	using namespace double_conversion;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_TRAILING_JUNK |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   0.0, 0.0, nullptr, nullptr};
	bool expectValue = true;
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
					throwParseError(fullExpr, expr.data() - fullExpr.data(), "The expression unexpected ends after unary operator."sv);
				}
			}
			if (expr[0] == '(') {
				expr.remove_prefix(1);
				OpenBracket({op, negative});
				expr = parse(expr, containIndexOrFunction, isRrf, fullExpr, joinedSelectors);
				if (expr.empty() || expr[0] != ')') {
					throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected ')'."sv);
				}
				expr.remove_prefix(1);
				CloseBracket();
			} else if (expr[0] == '"') {
				auto parsedIndexName = parseIndexName(expr, joinedSelectors, fullExpr);
				if (parsedIndexName.joinedSelectorIt == joinedSelectors.cend()) {
					skipSpaces();
					std::ignore = Append<SortExprFuncs::Index>({op, negative}, std::move(parsedIndexName.name));
				} else {
					auto dist = static_cast<size_t>(parsedIndexName.joinedSelectorIt - joinedSelectors.cbegin());
					std::ignore = Append<JoinedIndex>({op, negative}, dist, std::move(parsedIndexName.name));
				}
				*containIndexOrFunction = true;
			} else {
				int countOfCharsParsedAsDouble = 0;
				const double value = converter.StringToDouble(expr.data(), expr.size(), &countOfCharsParsedAsDouble);
				if (countOfCharsParsedAsDouble != 0) {
					std::ignore = Append<Value>({op, false}, negative ? -value : value);
					expr.remove_prefix(countOfCharsParsedAsDouble);
				} else {
					auto parsedIndexName = parseIndexName(expr, joinedSelectors, fullExpr);
					if (parsedIndexName.joinedSelectorIt == joinedSelectors.cend()) {
						skipSpaces();
						if (!expr.empty() && expr[0] == '(') {
							expr.remove_prefix(1);
							const auto funcName = toLower(parsedIndexName.name);
							if (funcName == "rank"sv) {
								parseRank(expr, joinedSelectors, fullExpr, op, negative, skipSpaces);
							} else if (funcName == "rrf"sv) {
								if (op != OpPlus) {
									throwParseError(fullExpr, expr.data() - fullExpr.data(),
													"Reciprocal rank fusion (RRF) could be only with operation '+'"sv);
								}
								if (negative) {
									throwParseError(fullExpr, expr.data() - fullExpr.data(),
													"Reciprocal rank fusion (RRF) cannot be with negative sign"sv);
								}
								skipSpaces();
								constexpr static auto rankConstName = "rank_const"sv;
								if (!expr.empty() && expr[0] != ')') {
									if (!checkIfStartsWith(rankConstName, expr)) {
										throwParseError(fullExpr, expr.data() - fullExpr.data(), "'rank_const' is expected"sv);
									}
									expr.remove_prefix(rankConstName.size());
									skipSpaces();
									if (expr.empty() || expr[0] != '=') {
										throwParseError(fullExpr, expr.data() - fullExpr.data(), "'=' is expected"sv);
									}
									expr.remove_prefix(1);
									skipSpaces();
									countOfCharsParsedAsDouble = 0;
									const double rankConst =
										converter.StringToDouble(expr.data(), expr.size(), &countOfCharsParsedAsDouble);
									if (countOfCharsParsedAsDouble == 0) {
										throwParseError(fullExpr, expr.data() - fullExpr.data(), "Rank constant of RRF is expected"sv);
									}
									expr.remove_prefix(countOfCharsParsedAsDouble);
									if (rankConst < 1.0) {
										throwParseError(fullExpr, expr.data() - fullExpr.data(), "Rank constant of RRF must be >= 1"sv);
									}
									skipSpaces();
									std::ignore = Append<Rrf>({OpPlus, false}, rankConst);
								} else {
									std::ignore = Append<Rrf>({OpPlus, false});
								}
								*isRrf = true;
							} else if (funcName == "abs"sv) {
								OpenBracket({op, negative}, true);
								expr = parse(expr, containIndexOrFunction, isRrf, fullExpr, joinedSelectors);
								CloseBracket();
							} else if (funcName == "st_distance"sv) {
								parseDistance(expr, joinedSelectors, fullExpr, op, negative, skipSpaces);
							} else if (funcName == "hash") {
								skipSpaces();
								uint32_t seed = 0;
								auto [ptr, ec] = std::from_chars(expr.data(), expr.data() + expr.size(), seed);
								if (ec == std::errc()) {
									std::ignore = Append({op, negative}, SortExprFuncs::SortHash{seed});
									expr.remove_prefix(ptr - expr.data());
								} else if (ec == std::errc::result_out_of_range) {
									throwParseError(fullExpr, expr.data() - fullExpr.data(), "Number is out of range"sv);
								} else {
									std::ignore = Append({op, negative}, SortExprFuncs::SortHash{});
								}
								skipSpaces();
							} else {
								throwParseError(fullExpr, expr.data() - fullExpr.data(), "Unsupported function name : '" + funcName + "'.");
							}
							if (expr.empty() || expr[0] != ')') {
								throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected ')'."sv);
							}
							expr.remove_prefix(1);
						} else {
							std::ignore = Append<SortExprFuncs::Index>({op, negative}, std::move(parsedIndexName.name));
						}
					} else {
						std::ignore = Append<JoinedIndex>({op, negative},
														  static_cast<size_t>(parsedIndexName.joinedSelectorIt - joinedSelectors.cbegin()),
														  std::move(parsedIndexName.name));
					}
					*containIndexOrFunction = true;
				}
			}
			expectValue = false;
		} else {
			switch (expr[0]) {
				case ')':
					return expr;
				case '+':
					op = OpPlus;
					lastOperationPlusOrMinus = true;
					break;
				case '-':
					op = OpMinus;
					lastOperationPlusOrMinus = true;
					break;
				case '*':
					op = OpMult;
					lastOperationPlusOrMinus = false;
					break;
				case '/':
					op = OpDiv;
					lastOperationPlusOrMinus = false;
					break;
				default:
					throwParseError(fullExpr, expr.data() - fullExpr.data(),
									std::string("Expected ')', '+', '-', '*' of '/', but obtained '") + expr[0] + "'.");
			}
			expr.remove_prefix(1);
			expectValue = true;
		}
		skipSpaces();
	}
	if (expectValue) {
		throwParseError(fullExpr, expr.data() - fullExpr.data(), "Expected value."sv);
	}
	return expr;
}

template <typename T>
SortExpression SortExpression::Parse(std::string_view expression, const std::vector<T>& joinedSelector) {
	SortExpression result;
	bool containIndexOrFunction = false;
	bool isRrf = false;
	const auto expr = result.parse(expression, &containIndexOrFunction, &isRrf, expression, joinedSelector);
	result.reduce();
	if (isRrf && result.Size() != 1) {
		throwParseError(expression, expr.data() - expression.data(), "Reciprocal rank fusion (RRF) must be single in sort expression"sv);
	}
	if (!expr.empty()) {
		throwParseError(expression, expr.data() - expression.data(), ""sv);
	}
	if (!containIndexOrFunction) {
		throwParseError(expression, expr.data() - expression.data(), "Sort expression does not depend from namespace data"sv);
	}
	return result;
}

template SortExpression SortExpression::Parse(std::string_view, const std::vector<JoinedSelector>&);
template SortExpression SortExpression::Parse(std::string_view, const std::vector<JoinedSelectorMock>&);
template SortExpression SortExpression::Parse(std::string_view, const std::vector<JoinedNsNameMock>&);
template SortExpression SortExpression::Parse(std::string_view, const std::vector<JoinedQuery>&);

[[noreturn]] void throwDivisionByZero() { throw Error(errQueryExec, "Division by zero in sort expression"sv); }

static double CalcSortHash(IdType rowId, uint32_t seed, uint32_t shardIdHash) noexcept {
	uint32_t hash;
	uint32_t r = uint32_t(rowId) ^ shardIdHash;
	MurmurHash3_x86_32(&r, sizeof(r), seed, &hash);
	return double(hash);
}

double SortExpression::calculate(const_iterator it, const_iterator end, IdType rowId, ConstPayload pv,
								 const joins::NamespaceResults* joinedResults, const std::vector<JoinedSelector>& js, RankT rank,
								 TagsMatcher& tagsMatcher, uint32_t shardIdHash) {
	assertrx_throw(it != end);
	assertrx_throw(it->operation.op == OpPlus);
	double totalResult = 0.0;
	double multResult = 0.0;

	for (; it != end; ++it) {
		double value = it->Visit(
			[&] RX_PRE_LMBD_ALWAYS_INLINE(const SortExpressionBracket& b) RX_POST_LMBD_ALWAYS_INLINE {
				const double res = calculate(it.cbegin(), it.cend(), rowId, pv, joinedResults, js, rank, tagsMatcher, shardIdHash);
				return (b.IsAbs() && res < 0) ? -res : res;
			},
			[] RX_PRE_LMBD_ALWAYS_INLINE(const Value& v) RX_POST_LMBD_ALWAYS_INLINE { return v.value; },
			[&pv, &tagsMatcher] RX_PRE_LMBD_ALWAYS_INLINE(const SortExprFuncs::Index& i)
				RX_POST_LMBD_ALWAYS_INLINE { return i.GetValue(pv, tagsMatcher); },
			[rowId, joinedResults, &js] RX_PRE_LMBD_ALWAYS_INLINE(const JoinedIndex& i) RX_POST_LMBD_ALWAYS_INLINE {
				assertrx_throw(joinedResults);
				return i.GetValue(rowId, *joinedResults, js);
			},
			[rank] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<Rank, RankNamed> auto&)
				RX_POST_LMBD_ALWAYS_INLINE { return double(rank.Value()); },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const Rrf&) RX_POST_LMBD_ALWAYS_INLINE -> double {
				throw Error(errNotValid, "Reciprocal rank fusion (RRF) is allowed in hybrid queries only");
			},
			[rowId, shardIdHash] RX_PRE_LMBD_ALWAYS_INLINE(const SortExprFuncs::SortHash& sortHash)
				RX_POST_LMBD_ALWAYS_INLINE { return CalcSortHash(rowId, sortHash.Seed(), shardIdHash); },
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
				totalResult += multResult;
				multResult = value;
				break;
			case OpMinus:
				totalResult += multResult;
				multResult = -value;
				break;
			case OpMult:
				multResult *= value;
				break;
			case OpDiv:
				if (fp::IsZero(value)) [[unlikely]] {
					throwDivisionByZero();
				}
				multResult /= value;
				break;
		}
	}
	return totalResult + multResult;
}

double ProxiedSortExpression::calculate(const_iterator it, const_iterator end, IdType rowId, ConstPayload pv, RankT rank,
										TagsMatcher& tagsMatcher, uint32_t shardIdHash) {
	assertrx(it != end);
	assertrx(it->operation.op == OpPlus);
	double totalResult = 0.0;
	double multResult = 0.0;
	for (; it != end; ++it) {
		double value = it->Visit(
			[&](const SortExpressionBracket& b) {
				const double res = calculate(it.cbegin(), it.cend(), rowId, pv, rank, tagsMatcher, shardIdHash);
				return (b.IsAbs() && res < 0) ? -res : res;
			},
			[](const Value& v) { return v.value; },
			[rowId, shardIdHash](const SortExprFuncs::SortHash& sh) { return CalcSortHash(rowId, sh.Seed(), shardIdHash); },
			[&pv, &tagsMatcher](const SortExprFuncs::ProxiedField& f) { return f.GetValue(pv, tagsMatcher); },
			[rank](const concepts::OneOf<Rank, RankNamed> auto&) { return double(rank.Value()); },
			[&pv, &tagsMatcher](const ProxiedDistanceFromPoint& f) { return f.GetValue(pv, tagsMatcher); },
			[&pv, &tagsMatcher](const ProxiedDistanceBetweenFields& f) { return f.GetValue(pv, tagsMatcher); });
		if (it->operation.negative) {
			value = -value;
		}
		switch (it->operation.op) {
			case OpPlus:
				totalResult += multResult;
				multResult = value;
				break;
			case OpMinus:
				totalResult += multResult;
				multResult = -value;
				break;
			case OpMult:
				multResult *= value;
				break;
			case OpDiv:
				if (fp::IsZero(value)) [[unlikely]] {
					throwDivisionByZero();
				}
				multResult /= value;
				break;
			default:
				throw_as_assert;
		}
	}
	return totalResult + multResult;
}

std::string ProxiedSortExpression::getJsonPath(std::string_view columnName, int idxNo, const NamespaceImpl& ns) {
	if (idxNo == SetByJsonPath) {
		return std::string(columnName);
	} else {
		assertrx_throw(idxNo >= 0);
		const auto& payloadType = *ns.payloadType_;
		assertrx_throw(idxNo < payloadType.NumFields());
		const auto& jsons = payloadType.Field(idxNo).JsonPaths();
		assertrx_throw(jsons.size() == 1);
		return jsons[0];
	}
}

void ProxiedSortExpression::fill(SortExpression::const_iterator it, SortExpression::const_iterator endIt, const NamespaceImpl& ns) {
	for (; it != endIt; ++it) {
		it->Visit(
			[](const auto&) { throw Error{errQueryExec, "JOIN is unsupported in proxied query"sv}; },
			[&](const SortExpressionBracket& b) {
				OpenBracket(it->operation, b.IsAbs());
				fill(it.cbegin(), it.cend(), ns);
				CloseBracket();
			},
			[&](const Value& v) { std::ignore = Append(it->operation, v); },
			[&](const SortExprFuncs::Index& i) { std::ignore = Append<ProxiedField>(it->operation, getJsonPath(i.column, i.index, ns)); },
			[&](const Rank&) { std::ignore = Append<Rank>(it->operation); },
			[&](const RankNamed& r) { std::ignore = Append(it->operation, r); },
			[&](const SortExprFuncs::SortHash& sh) { std::ignore = Append(it->operation, sh); },
			[&](const DistanceFromPoint& i) {
				std::ignore = Append<ProxiedDistanceFromPoint>(it->operation, getJsonPath(i.column, i.index, ns), i.point);
			},
			[&](const DistanceBetweenIndexes& i) {
				std::ignore = Append<ProxiedDistanceBetweenFields>(it->operation, getJsonPath(i.column1, i.index1, ns),
																   getJsonPath(i.column2, i.index2, ns));
			});
	}
}

void SortExpression::PrepareIndexes(const NamespaceImpl& ns) {
	VisitForEach(
		Skip<SortExpressionOperation, SortExpressionBracket, SortExprFuncs::Value, JoinedIndex, Rank, Rrf, SortExprFuncs::SortHash,
			 DistanceJoinedIndexFromPoint, DistanceBetweenIndexAndJoinedIndex, DistanceBetweenJoinedIndexes,
			 DistanceBetweenJoinedIndexesSameNs>{},
		[&ns](SortExprFuncs::Index& exprIndex) { PrepareSortIndex(exprIndex.column, exprIndex.index, ns, IsRanked_False); },
		[&ns](DistanceFromPoint& exprIndex) { PrepareSortIndex(exprIndex.column, exprIndex.index, ns, IsRanked_False); },
		[&ns](RankNamed& exprRank) { PrepareSortIndex(exprRank.IndexName(), exprRank.IndexNoRef(), ns, IsRanked_True); },
		[&ns](DistanceBetweenIndexes& exprIndex) {
			PrepareSortIndex(exprIndex.column1, exprIndex.index1, ns, IsRanked_False);
			PrepareSortIndex(exprIndex.column2, exprIndex.index2, ns, IsRanked_False);
		});
}

void SortExpression::PrepareSortIndex(std::string& column, int& indexNo, const NamespaceImpl& ns, IsRanked isRanked) {
	assertrx_throw(indexNo == IndexValueType::NotSet);
	assertrx_throw(!column.empty());
	indexNo = IndexValueType::SetByJsonPath;
	if (ns.tryGetIndexByNameOrJsonPath(column, indexNo)) {
		const auto& index = *ns.indexes_[indexNo];
		if (isRanked) {
			if (!index.IsFloatVector() && !IsFullText(index.Type())) {
				throw Error(errQueryExec, "Ordering by rank allowed by fulltext or float vector index only: '{}'", column);
			}
		} else if (index.IsFloatVector()) {
			throw Error(errQueryExec, "Ordering by float vector index is not allowed: '{}'", column);
		}
		if (index.Opts().IsSparse()) {
			assertrx_dbg(indexNo < ns.indexes_.firstCompositePos());
			const auto& fields = index.Fields();
			assertrx_dbg(fields.getJsonPathsLength() == 1);
			column = fields.getJsonPath(0);
			indexNo = IndexValueType::SetByJsonPath;
		}
	}
}

Changed SortExpression::constantsFirstInMultiplications(iterator from, iterator to) {
	auto multStart = from;
	auto moveStart = from;
	Changed changed = Changed_False;
	for (auto it = from; it != to; ++it) {
		if (it->IsSubTree()) {
			changed |= constantsFirstInMultiplications(it.begin(), it.end());
		}
		const auto op = it->operation;
		if (op.op == OpPlus || op.op == OpMinus) {
			multStart = it;
			moveStart = it;
		} else if (it->Is<Value>()) {
			Value& val = it->Value<Value>();
			if (op.negative) {
				val.value = -val.value;
				it->operation.negative = false;
				changed = Changed_True;
			}
			if (op.op == OpDiv) {
				if (fp::IsZero(val.value)) [[unlikely]] {
					throwDivisionByZero();
				}
				val.value = 1.0 / val.value;
				it->operation.op = OpMult;
				changed = Changed_True;
			}
			if (moveStart->Is<Value>()) {
				moveStart = it;
			} else {
				const double value = val.value;
				for (auto moveEnd = moveStart.PlainIterator(), moveIt = it.PlainIterator(); moveIt != moveEnd; --moveIt) {
					*moveIt = std::move(*(moveIt - 1));
				}
				moveStart->SetValue<Value>(value);
				if (moveStart == multStart) {
					auto next = moveStart;
					++next;
					moveStart->operation = next->operation;
					next->operation = {OpMult, false};
				}
				++moveStart;
				changed = Changed_True;
			}
		} else if (moveStart->Is<Value>()) {
			moveStart = it;
		}
	}
	return changed;
}

bool SortExpression::justMultiplications(size_t begin, size_t end) {
	for (size_t i = Next(begin); i < end; i = Next(i)) {
		const auto op = GetOperation(i).op;
		if (op == OpPlus || op == OpMinus) {
			return false;
		}
	}
	return true;
}

bool SortExpression::justMultiplicationsOfConstants(size_t begin, size_t end) {
	if (!Is<Value>(begin)) {
		return false;
	}
	for (size_t i = Next(begin); i < end; i = Next(i)) {
		if (!Is<Value>(i)) {
			return false;
		}
		const auto op = GetOperation(i).op;
		if (op == OpPlus || op == OpMinus) {
			return false;
		}
	}
	return true;
}

size_t SortExpression::removeBrackets(size_t begin, size_t end) {
	if (begin != end) {
		const auto firstOp = GetOperation(begin).op;
		if (firstOp != OpPlus && firstOp != OpMinus) {
			throw Error{errQueryExec, "Multiplication or division at the start of sort expression or after opening bracket"};
		}
	}
	size_t deleted = 0;
	for (size_t i = begin, next; i < end - deleted; i = next) {
		next = Next(i);
		if (!IsSubTree(i)) {
			continue;
		}
		const size_t firstInBracket = i + 1;
		if (Size(i) < 2) {
			Erase(i, firstInBracket);
			++deleted;
			next = i;
			continue;
		}
		{
			const auto deletedInBracket = removeBrackets(firstInBracket, next);
			deleted += deletedInBracket;
			assertrx_throw(i + deletedInBracket < next);
			next -= deletedInBracket;
		}
		if (auto& bracket = Get<SortExpressionBracket>(i); bracket.IsAbs()) {
			if (!justMultiplicationsOfConstants(firstInBracket, next)) {
				continue;
			}
			for (size_t j = firstInBracket; j < next; j = Next(j)) {
				double& value = Get<Value>(j).value;
				value = std::abs(value);
				auto op = GetOperation(j);
				op.negative = false;
				SetOperation(op, j);
			}
			SetOperation({OpPlus, false}, firstInBracket);
			bracket.SetAbs(false);
		}
		const auto curOp = GetOperation(i);
		const auto nextOp = (next < end - deleted) ? std::optional{GetOperation(next)} : std::nullopt;
		if (curOp.op == OpMult || curOp.op == OpDiv) {
			if (!justMultiplications(firstInBracket, next)) {
				continue;
			}
			auto& firstOp = container_[firstInBracket].operation;
			if (curOp.negative != (firstOp.op == OpMinus)) {
				firstOp.negative = !firstOp.negative;
			}
			firstOp.op = curOp.op;
			if (curOp.op == OpDiv) {
				for (size_t j = Next(firstInBracket); j < next; j = Next(j)) {
					auto& op = container_[j].operation.op;
					op = op == OpMult ? OpDiv : OpMult;
				}
			}
		} else if (nextOp && (nextOp->op == OpMult || nextOp->op == OpDiv)) {
			if (!justMultiplications(firstInBracket, next)) {
				continue;
			}
			if (curOp.negative != (curOp.op == OpMinus)) {
				container_[firstInBracket].operation.negative = !container_[firstInBracket].operation.negative;
			}
		} else if (curOp.negative != (curOp.op == OpMinus)) {
			for (size_t j = firstInBracket; j < next; j = Next(j)) {
				auto& op = container_[j].operation;
				if (op.op == OpPlus || op.op == OpMinus) {
					op.negative = !op.negative;
				}
			}
		}
		Erase(i, firstInBracket);
		++deleted;
		next = i;
	}
	return deleted;
}

Changed SortExpression::reduceNegatives(size_t begin, size_t end) {
	Changed changed = Changed_False;
	struct {
		std::decay_t<decltype(container_[0])>* node{nullptr};
		bool isConst{false};
	} multChain;
	for (size_t i = begin; i < end; i = Next(i)) {
		if (IsSubTree(i)) {
			changed |= reduceNegatives(i + 1, Next(i));
		}
		auto& op = container_[i].operation;
		if (op.op == OpPlus || op.op == OpMinus) {
			multChain.node = &container_[i];
			multChain.isConst = Is<Value>(i);
		} else if (!multChain.isConst && Is<Value>(i)) {
			if (op.negative != multChain.node->operation.negative) {
				auto& value = Get<Value>(i).value;
				value = -value;
				op.negative = multChain.node->operation.negative = false;
				changed = Changed_True;
			} else if (op.negative) {
				op.negative = multChain.node->operation.negative = false;
				changed = Changed_True;
			}
			multChain.node = &container_[i];
			multChain.isConst = true;
		}
		if (Is<Value>(i)) {
			if (op.negative != (op.op == OpMinus)) {
				auto& value = Get<Value>(i).value;
				value = -value;
			}
			op.negative = false;
			if (op.op == OpMinus) {
				op.op = OpPlus;
			}
		}
		if (op.negative && i != begin) {
			switch (op.op) {
				case OpPlus:
					op.op = OpMinus;
					break;
				case OpMinus:
					op.op = OpPlus;
					break;
				case OpMult:
				case OpDiv:
					if (multChain.isConst) {
						auto& value = multChain.node->Value<Value>().value;
						value = -value;
					} else {
						multChain.node->operation.negative = !multChain.node->operation.negative;
					}
					break;
				default:
					throw_as_assert;
			}
			op.negative = false;
			changed = Changed_True;
		}
	}
	if (auto& op = container_[begin].operation; op.op == OpMinus) {
		op.op = OpPlus;
		op.negative = !op.negative;
		changed = Changed_True;
	}
	return changed;
}

void SortExpression::reduce() {
	Changed changed = Changed_True;
	for (size_t i = 0; i < 100 && changed; ++i) {
		changed = constantsFirstInMultiplications(begin(), end());
		changed |= multiplyConstants();
		changed |= sumConstants();
		changed |= (removeBrackets(0, Size()) != 0);
		changed |= reduceNegatives(0, Size());
	}
}

void SortExpression::ThrowNonReranker() {
	throw Error(errNotValid, "In hybrid query ordering expression should be 'RRF()' or in form 'a * rank(index1) + b * rank(index2) + c'");
}

void SortExpression::initRerankerRank(size_t pos, int& indexNo, double& k, double& defaultValue) const {
	const auto op_0 = GetOperation(pos);
	if (op_0.op != OpPlus && op_0.op != OpMinus) {
		ThrowNonReranker();
	}
	if (!Is<Value>(pos)) {
		ThrowNonReranker();
	}
	const auto op_1 = GetOperation(pos + 1);
	if (op_1.op != OpMult) {
		ThrowNonReranker();
	}
	k = ((op_0 == OpMinus) != (op_0.negative != op_1.negative)) ? -Get<Value>(pos).value : Get<Value>(pos).value;
	if (!Is<RankNamed>(pos + 1)) {
		ThrowNonReranker();
	}
	const auto& rankNamed = Get<RankNamed>(pos + 1);
	indexNo = rankNamed.IndexNo();
	defaultValue = rankNamed.DefaultValue();
}

void SortExpression::initRerankerRankSingle(size_t pos, int& indexNo, double& k, double& defaultValue) const {
	assertrx_throw(Is<RankNamed>(pos));
	const auto op = GetOperation(pos);
	if (op.op != OpPlus && op.op != OpMinus) {
		ThrowNonReranker();
	}
	k = ((op == OpMinus) == op.negative) ? 1.0 : -1.0;
	const auto& rankNamed = Get<RankNamed>(pos);
	indexNo = rankNamed.IndexNo();
	defaultValue = rankNamed.DefaultValue();
}

void SortExpression::initRerankerConst(size_t pos, double& c) const {
	const auto op = GetOperation(pos);
	if (op.op != OpPlus && op.op != OpMinus) {
		ThrowNonReranker();
	}
	if (!Is<Value>(pos)) {
		ThrowNonReranker();
	}
	c = ((op.op == OpMinus) != op.negative) ? -Get<Value>(pos).value : Get<Value>(pos).value;
}

Reranker SortExpression::ToReranker(const NamespaceImpl& ns, Desc desc) const {
	if (Size() == 1 && Is<Rrf>(0)) {
		return {RerankerRRF{Get<Rrf>(0).RankConst()}, desc};
	}
	if (Size() < 2) {
		ThrowNonReranker();
	}
	double k1, k2, c;
	int idxNo1, idxNo2;
	double default1, default2;
	if (Is<RankNamed>(0)) {
		initRerankerRankSingle(0, idxNo1, k1, default1);
		if (Is<RankNamed>(1)) {
			initRerankerRankSingle(1, idxNo2, k2, default2);
			if (Size() == 2) {
				c = 0.0;
			} else {
				if (Size() != 3) {
					ThrowNonReranker();
				}
				initRerankerConst(2, c);
			}
		} else {
			if (Size() < 3) {
				ThrowNonReranker();
			}
			if (const auto op_2 = GetOperation(2); op_2.op == OpPlus || op_2.op == OpMinus) {
				initRerankerConst(1, c);
				if (Is<RankNamed>(2)) {
					if (Size() != 3) {
						ThrowNonReranker();
					}
					initRerankerRankSingle(2, idxNo2, k2, default2);
				} else {
					if (Size() != 4) {
						ThrowNonReranker();
					}
					initRerankerRank(2, idxNo2, k2, default2);
				}
			} else {
				initRerankerRank(1, idxNo2, k2, default2);
				if (Size() == 3) {
					c = 0.0;
				} else {
					if (Size() != 4) {
						ThrowNonReranker();
					}
					initRerankerConst(3, c);
				}
			}
		}
	} else if (const auto op_1 = GetOperation(1); op_1.op == OpPlus || op_1.op == OpMinus) {
		initRerankerConst(0, c);
		if (Is<RankNamed>(1)) {
			if (Size() < 3) {
				ThrowNonReranker();
			}
			initRerankerRankSingle(1, idxNo1, k1, default1);
			if (Is<RankNamed>(2)) {
				if (Size() != 3) {
					ThrowNonReranker();
				}
				initRerankerRankSingle(2, idxNo2, k2, default2);
			} else {
				if (Size() != 4) {
					ThrowNonReranker();
				}
				initRerankerRank(2, idxNo2, k2, default2);
			}
		} else {
			if (Size() < 4) {
				ThrowNonReranker();
			}
			initRerankerRank(1, idxNo1, k1, default1);
			if (Is<RankNamed>(3)) {
				if (Size() != 4) {
					ThrowNonReranker();
				}
				initRerankerRankSingle(3, idxNo2, k2, default2);
			} else {
				if (Size() != 5) {
					ThrowNonReranker();
				}
				initRerankerRank(3, idxNo2, k2, default2);
			}
		}
	} else {
		if (Size() < 3) {
			ThrowNonReranker();
		}
		initRerankerRank(0, idxNo1, k1, default1);
		if (Is<RankNamed>(2)) {
			initRerankerRankSingle(2, idxNo2, k2, default2);
			if (Size() == 3) {
				c = 0.0;
			} else {
				if (Size() != 4) {
					ThrowNonReranker();
				}
				initRerankerConst(3, c);
			}
		} else {
			if (Size() < 4) {
				ThrowNonReranker();
			}
			if (const auto op_3 = GetOperation(3); op_3.op == OpPlus || op_3.op == OpMinus) {
				initRerankerConst(2, c);
				if (Is<RankNamed>(3)) {
					if (Size() != 4) {
						ThrowNonReranker();
					}
					initRerankerRankSingle(3, idxNo2, k2, default2);
				} else {
					if (Size() != 5) {
						ThrowNonReranker();
					}
					initRerankerRank(3, idxNo2, k2, default2);
				}
			} else {
				initRerankerRank(2, idxNo2, k2, default2);
				if (Size() == 4) {
					c = 0.0;
				} else {
					if (Size() != 5) {
						ThrowNonReranker();
					}
					initRerankerConst(4, c);
				}
			}
		}
	}
	assertrx_throw(size_t(idxNo1) < ns.indexes_.size());
	assertrx_throw(size_t(idxNo2) < ns.indexes_.size());
	if (ns.indexes_[idxNo1]->IsFloatVector()) {
		if (!ns.indexes_[idxNo2]->IsFulltext()) {
			ThrowNonReranker();
		}
		return {RerankerLinear{k1, default1, k2, default2, c}, desc};
	} else {
		if (!ns.indexes_[idxNo1]->IsFulltext() || !ns.indexes_[idxNo2]->IsFloatVector()) {
			ThrowNonReranker();
		}
		return {RerankerLinear{k2, default2, k1, default1, c}, desc};
	}
}

Reranker Reranker::Default() noexcept { return {RerankerRRF(SortExprFuncs::Rrf::kDefaultRankConst), Desc_True}; }

std::string SortExpression::Dump() const {
	WrSerializer ser;
	dump(cbegin(), cend(), ser);
	return std::string{ser.Slice()};
}

void SortExpression::dump(const_iterator begin, const_iterator end, WrSerializer& ser) {
	assertf(begin->operation.op == OpPlus, "{}", int(begin->operation.op));
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
			ser << "(-"sv;
		}
		it->Visit(
			[&it, &ser](const SortExpressionBracket& b) {
				ser << (b.IsAbs() ? "ABS("sv : "("sv);
				dump(it.cbegin(), it.cend(), ser);
				ser << ')';
			},
			[&ser](const Value& v) { ser << v.value; }, [&ser](const SortExprFuncs::Index& i) { ser << i.column; },
			[&ser](const JoinedIndex& i) { ser << "joined "sv << i.nsIdx << ' ' << i.column; }, [&ser](const Rank&) { ser << "rank()"sv; },
			[&ser](const RankNamed& r) { ser << "rank("sv << r.IndexName() << ", "sv << r.DefaultValue() << ')'; },
			[&ser](const Rrf& r) { ser << "RRF(rank_const="sv << r.RankConst() << ')'; },
			[&ser](const SortExprFuncs::SortHash& sh) { ser << "hash("sv << (sh.IsUserSeed() ? std::to_string(sh.Seed()) : ""sv) << ')'; },
			[&ser](const DistanceFromPoint& i) {
				ser << "ST_Distance("sv << i.column << ", ["sv << i.point.X() << ", "sv << i.point.Y() << "])"sv;
			},
			[&ser](const DistanceJoinedIndexFromPoint& i) {
				ser << "ST_Distance(joined "sv << i.nsIdx << ' ' << i.column << ", ["sv << i.point.X() << ", "sv << i.point.Y() << "])"sv;
			},
			[&ser](const DistanceBetweenIndexes& i) { ser << "ST_Distance("sv << i.column1 << ", "sv << i.column2 << ')'; },
			[&ser](const DistanceBetweenIndexAndJoinedIndex& i) {
				ser << "ST_Distance("sv << i.column << ", joined "sv << i.jNsIdx << ' ' << i.jColumn << ')';
			},
			[&ser](const DistanceBetweenJoinedIndexes& i) {
				ser << "ST_Distance(joined "sv << i.nsIdx1 << ' ' << i.column1 << ", joined "sv << i.nsIdx2 << ' ' << i.column2 << ')';
			},
			[&ser](const DistanceBetweenJoinedIndexesSameNs& i) {
				ser << "ST_Distance(joined "sv << i.nsIdx << ' ' << i.column1 << ", joined "sv << i.nsIdx << ' ' << i.column2 << ')';
			});
		if (it->operation.negative) {
			ser << ')';
		}
	}
}

std::string ProxiedSortExpression::Dump() const {
	WrSerializer ser;
	dump(cbegin(), cend(), ser);
	return std::string{ser.Slice()};
}

void ProxiedSortExpression::dump(const_iterator begin, const_iterator end, WrSerializer& ser) {
	assertrx(begin->operation.op == OpPlus);
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
			ser << "(-"sv;
		}
		it->Visit(
			[&it, &ser](const SortExpressionBracket& b) {
				ser << (b.IsAbs() ? "ABS("sv : "("sv);
				dump(it.cbegin(), it.cend(), ser);
				ser << ')';
			},
			[&ser](const Value& v) { ser << v.value; }, [&ser](const SortExprFuncs::ProxiedField& i) { ser << i.json; },
			[&ser](const Rank&) { ser << "rank()"sv; }, [&ser](const RankNamed& r) { ser << "rank("sv << r.IndexName() << ')'; },
			[&ser](const SortExprFuncs::SortHash& sh) { ser << "hash("sv << (sh.IsUserSeed() ? std::to_string(sh.Seed()) : ""sv) << ')'; },
			[&ser](const ProxiedDistanceFromPoint& i) {
				ser << "ST_Distance("sv << i.json << ", ["sv << i.point.X() << ", "sv << i.point.Y() << "])"sv;
			},
			[&ser](const ProxiedDistanceBetweenFields& i) { ser << "ST_Distance("sv << i.json2 << ", "sv << i.json2 << ')'; });
		if (it->operation.negative) {
			ser << ')';
		}
	}
}

std::ostream& operator<<(std::ostream& os, const SortExpression& se) { return os << se.Dump(); }
std::ostream& operator<<(std::ostream& os, const ProxiedSortExpression& se) { return os << se.Dump(); }

}  // namespace reindexer
