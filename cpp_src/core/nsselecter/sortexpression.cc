#include "sortexpression.h"
#include "core/index/index.h"
#include "tools/stringstools.h"
#include "vendor/double-conversion/double-conversion.h"

namespace reindexer {

static void throwParseError() { throw Error(errParams, "Sort expression parse failed"); }

double SortExpressionValue::GetValue(ConstPayload pv, uint8_t proc, TagsMatcher& tagsMatcher) const {
	switch (type) {
		case Value:
			return value;
		case Index: {
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
		case Rank:
			return proc;
	}
	assert(0);
	return 0.0;
}

bool operator==(const SortExpressionValue& lhs, const SortExpressionValue& rhs) {
	if (lhs.type != rhs.type) return false;
	switch (lhs.type) {
		case SortExpressionValue::Index:
			return lhs.column == rhs.column && lhs.index == rhs.index;
		case SortExpressionValue::Value:
			return lhs.value == rhs.value;
		case SortExpressionValue::Rank:
			return true;
	}
	assert(0);
	return false;
}

bool operator==(const SortExpressionOperation& lhs, const SortExpressionOperation& rhs) {
	return lhs.op == rhs.op && lhs.negative == rhs.negative;
}

SortExpression SortExpression::Parse(string_view expression) {
	SortExpression result;
	bool containIndexOrFunction = false;
	if (result.parse(expression.begin(), expression.end(), &containIndexOrFunction) != expression.end()) throwParseError();
	if (!containIndexOrFunction) throwParseError();
	return result;
}

void SortExpression::openBracketBeforeLastAppended() {
	const size_t pos = lastAppendedElement();
	assert(activeBrackets_.empty() || activeBrackets_.back() < pos);
	for (unsigned i : activeBrackets_) {
		assert(i < container_.size());
		container_[i]->Append();
	}
	const ArithmeticOpType op = container_[pos]->Op.op;
	container_[pos]->Op.op = OpPlus;
	activeBrackets_.push_back(pos);
	container_.insert(container_.begin() + pos, {{op, false}, container_.size() - pos + 1});
}

string_view::iterator SortExpression::parse(string_view::iterator it, string_view::iterator end, bool* containIndexOrFunction) {
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
			if (*it == '-') {
				if (lastOperationPlusOrMinus) {
					op = (op == OpPlus) ? OpMinus : OpPlus;
				} else {
					negative = true;
				}
				++it;
				skipSpaces();
				if (it == end) throwParseError();
			}
			if (*it == '(') {
				++it;
				OpenBracket({op, negative});
				it = parse(it, end, containIndexOrFunction);
				if (it == end || *it != ')') throwParseError();
				++it;
				CloseBracket();
			} else {
				int count = 0;
				const double value = converter.StringToDouble(it, end - it, &count);
				if (count != 0) {
					Append({op, false}, {negative ? -value : value});
					it += count;
				} else {
					const auto start = it;
					while (it != end && !isspace(*it) && *it != '(' && *it != ')') ++it;
					const string_view name{start, static_cast<size_t>(it - start)};
					skipSpaces();
					if (it != end && *it == '(') {
						if (!iequals(name, "rank")) throwParseError();
						++it;
						skipSpaces();
						if (it == end || *it != ')') throwParseError();
						++it;
						Append({op, negative}, {});
					} else {
						Append({op, negative}, {name});
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
					throwParseError();
			}
			++it;
			expectValue = true;
		}
		skipSpaces();
	}
	if (expectValue) throwParseError();
	if (needCloseBracket) CloseBracket();
	return it;
}

double SortExpression::calculate(const_iterator it, const_iterator end, ConstPayload pv, uint8_t proc, TagsMatcher& tagsMatcher) {
	assert(it != end);
	assert(it->Op.op == OpPlus);
	double result = 0.0;
	for (; it != end; ++it) {
		double value;
		if (it->IsLeaf()) {
			value = it->Value().GetValue(pv, proc, tagsMatcher);
		} else {
			value = calculate(it.cbegin(), it.cend(), pv, proc, tagsMatcher);
		}
		if (it->Op.negative) value = -value;
		switch (it->Op.op) {
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
	assert(begin->Op.op == OpPlus);
	for (const_iterator it = begin; it != end; ++it) {
		if (it != begin) {
			ser << ' ';
			switch (it->Op.op) {
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
		if (it->Op.negative) ser << "(-";
		if (it->IsLeaf()) {
			const SortExpressionValue& value = it->Value();
			switch (value.type) {
				case SortExpressionValue::Index:
					ser << value.column;
					break;
				case SortExpressionValue::Value:
					ser << value.value;
					break;
				case SortExpressionValue::Rank:
					ser << "rank()";
					break;
			}
		} else {
			ser << '(';
			dump(it.cbegin(), it.cend(), ser);
			ser << ')';
		}
		if (it->Op.negative) ser << ')';
	}
}

std::ostream& operator<<(std::ostream& os, const SortExpression& se) { return os << se.Dump(); }

}  // namespace reindexer
