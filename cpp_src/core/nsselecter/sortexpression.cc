#include "sortexpression.h"
#include <set>
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "joinedselector.h"
#include "joinedselectormock.h"
#include "tools/stringstools.h"
#include "vendor/double-conversion/double-conversion.h"

namespace reindexer {

static void throwParseError(string_view sortExpr, string_view::iterator pos, string_view message) {
	throw Error(errParams, "'%s' is not valid sort expression. Parser failed at position %d.%s%s", sortExpr, pos - sortExpr.begin(),
				message.empty() ? "" : " ", message);
}

bool SortExpression::ByIndexField() const {
	static constexpr SortExpressionOperation noOperation;
	return Size() == 1 && IsValue(0) && container_[0].Holds<SortExpressionIndex>() && GetOperation(0) == noOperation;
}

double SortExpressionIndex::GetValue(ConstPayload pv, TagsMatcher& tagsMatcher) const {
	VariantArray va;
	if (index == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(column, tagsMatcher, va, KeyValueUndefined);
	} else {
		pv.Get(index, va);
	}
	if (va.empty()) throw Error(errQueryExec, "Empty field in sort expression: %s", column);
	if (va.size() > 1 || va[0].Type() == KeyValueComposite || va[0].Type() == KeyValueTuple) {
		throw Error(errQueryExec, "Array, composite or tuple field in sort expression");
	}
	return va[0].As<double>();
}

double SortExpressionJoinedIndex::GetValue(IdType rowId, joins::NamespaceResults& joinResults,
										   const std::vector<JoinedSelector>& joinedSelectors) const {
	assert(joinedSelectors.size() > fieldIdx);
	const auto& js = joinedSelectors[fieldIdx];
	const auto& offsets = joinResults.offsets_[rowId];
	const auto offsetIt =
		std::find_if(offsets.cbegin(), offsets.cend(), [this](const joins::ItemOffset& offset) { return offset.field == fieldIdx; });
	if (offsetIt == offsets.cend() || offsetIt->size == 0) throw Error(errQueryExec, "Not found value joined from ns %s", js.RightNsName());
	if (offsetIt->size > 1) throw Error(errQueryExec, "Found more than 1 value joined from ns %s", js.RightNsName());
	const auto item = joinResults.items_[offsetIt->offset];
	assert(item.ValueInitialized());
	ConstPayload pv{js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.payloadType : js.rightNs_->payloadType_,
					item.Value()};
	VariantArray va;
	if (index == IndexValueType::SetByJsonPath) {
		pv.GetByJsonPath(
			column, js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.tagsMatcher : js.rightNs_->tagsMatcher_,
			va, KeyValueUndefined);
	} else {
		pv.Get(index, va);
	}
	if (va.empty()) throw Error(errQueryExec, "Empty field in sort expression: %s %s", js.RightNsName(), column);
	if (va.size() > 1 || va[0].Type() == KeyValueComposite || va[0].Type() == KeyValueTuple) {
		throw Error(errQueryExec, "Array, composite or tuple field in sort expression");
	}
	return va[0].As<double>();
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
string_view::iterator SortExpression::parse(string_view::iterator it, string_view::iterator end, bool* containIndexOrFunction,
											string_view fullExpr, const std::vector<T>& joinedSelectors) {
	static const std::set<char> allowedSymbolsInIndexName{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
		'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
		'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', '+'};
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
					Append({op, false}, SortExpressionValue{negative ? -value : value});
					it += countOfCharsParsedAsDouble;
				} else {
					auto start = it;
					auto joinedSelectorIt = joinedSelectors.cend();
					while (it != end && *it != '.' && allowedSymbolsInIndexName.find(*it) != allowedSymbolsInIndexName.end()) ++it;
					if (it != end && *it == '.') {
						const string_view namespaceName = {start, static_cast<size_t>(it - start)};
						++it;
						joinedSelectorIt = std::find_if(joinedSelectors.cbegin(), joinedSelectors.cend(),
														[namespaceName](const T& js) { return namespaceName == js.RightNsName(); });
						if (joinedSelectorIt != joinedSelectors.cend()) {
							if (std::find_if(joinedSelectorIt + 1, joinedSelectors.cend(), [namespaceName](const T& js) {
									return namespaceName == js.RightNsName();
								}) != joinedSelectors.cend()) {
								throwParseError(
									fullExpr, it,
									"Sorting by namespace which has been joined more than once: '" + string(namespaceName) + "'.");
							}
							start = it;
						}
					}
					while (it != end && allowedSymbolsInIndexName.find(*it) != allowedSymbolsInIndexName.end()) ++it;
					const string_view name{start, static_cast<size_t>(it - start)};
					if (name.empty()) throwParseError(fullExpr, it, "Expected index of function name.");
					if (joinedSelectorIt == joinedSelectors.cend()) {
						skipSpaces();
						if (it != end && *it == '(') {
							++it;
							const auto funcName = toLower(name);
							if (funcName == "rank") {
								Append({op, negative}, SortExpressionFuncRank{});
								skipSpaces();
							} else if (funcName == "abs") {
								OpenBracket({op, negative}, true);
								it = parse(it, end, containIndexOrFunction, fullExpr, joinedSelectors);
								CloseBracket();
							} else {
								throwParseError(fullExpr, it, "Unsupported function name : '" + funcName + "'.");
							}
							if (it == end || *it != ')') throwParseError(fullExpr, it, "Expected ')'.");
							++it;
						} else {
							Append({op, negative}, SortExpressionIndex{name});
						}
					} else {
						Append({op, negative},
							   SortExpressionJoinedIndex{static_cast<size_t>(joinedSelectorIt - joinedSelectors.cbegin()), name});
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
								 joins::NamespaceResults& joinedResults, const std::vector<JoinedSelector>& js, uint8_t proc,
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
			[](const SortExpressionValue& v) { return v.value; },
			[&pv, &tagsMatcher](const SortExpressionIndex& i) { return i.GetValue(pv, tagsMatcher); },
			[rowId, &joinedResults, &js](const SortExpressionJoinedIndex& i) { return i.GetValue(rowId, joinedResults, js); },
			[proc](const SortExpressionFuncRank&) -> double { return proc; });
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
			[&ser](const SortExpressionValue& v) { ser << v.value; }, [&ser](const SortExpressionIndex& i) { ser << i.column; },
			[&ser](const SortExpressionJoinedIndex& i) { ser << "joined " << i.fieldIdx << ' ' << i.column; },
			[&ser](const SortExpressionFuncRank&) { ser << "rank()"; });
		if (it->operation.negative) ser << ')';
	}
}

std::ostream& operator<<(std::ostream& os, const SortExpression& se) { return os << se.Dump(); }

}  // namespace reindexer
