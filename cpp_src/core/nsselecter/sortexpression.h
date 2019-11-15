#pragma once

#include "core/expressiontree.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

class Index;

struct SortExpressionValue {
	SortExpressionValue() : type{Rank} {}
	SortExpressionValue(double v) : type{Value}, value{v} {}
	SortExpressionValue(string_view c) : type{Index}, column{c} {}

	double GetValue(ConstPayload, uint8_t proc, TagsMatcher&) const;

	enum Type { Index, Value, Rank } type = Index;
	string_view column;
	int index = IndexValueType::NotSet;
	double value = 0.0;
};
bool operator==(const SortExpressionValue&, const SortExpressionValue&);

struct SortExpressionOperation {
	ArithmeticOpType op;
	bool negative;
};
bool operator==(const SortExpressionOperation&, const SortExpressionOperation&);

extern template bool ExpressionTree<SortExpressionValue, SortExpressionOperation, 2>::Leaf::IsEqual(const Node&) const;
class SortExpression : public ExpressionTree<SortExpressionValue, SortExpressionOperation, 2> {
public:
	static SortExpression Parse(string_view);
	double Calculate(ConstPayload pv, uint8_t proc, TagsMatcher& tagsMatcher) const {
		return calculate(cbegin(), cend(), pv, proc, tagsMatcher);
	}
	bool JustByIndex() const {
		static constexpr SortExpressionOperation noOperation{OpPlus, false};
		return Size() == 1 && IsValue(0) && operator[](0).type == SortExpressionValue::Index && GetOperation(0) == noOperation;
	}
	bool ContainRank() const {
		bool result = false;
		ForEachValue([&result](const SortExpressionValue& v, SortExpressionOperation) {
			if (v.type == SortExpressionValue::Rank) result = true;
		});
		return result;
	}

	std::string Dump() const;

private:
	string_view::iterator parse(string_view::iterator begin, string_view::iterator end, bool* containIndexOrFunction);
	static double calculate(const_iterator begin, const_iterator end, ConstPayload, uint8_t proc, TagsMatcher&);

	void openBracketBeforeLastAppended();
	static void dump(const_iterator begin, const_iterator end, WrSerializer&);
};
std::ostream& operator<<(std::ostream&, const SortExpression&);

}  // namespace reindexer
