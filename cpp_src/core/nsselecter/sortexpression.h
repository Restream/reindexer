#pragma once

#include "core/expressiontree.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

class Index;
class JoinedSelector;

namespace joins {
class NamespaceResults;
}  // namespace joins

struct SortExpressionValue {
	SortExpressionValue(double v) : value{v} {}
	bool operator==(const SortExpressionValue& other) const noexcept { return value == other.value; }

	double value;
};

struct SortExpressionIndex {
	SortExpressionIndex(string_view c) : column{c}, index{IndexValueType::NotSet} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const SortExpressionIndex& other) const noexcept { return column == other.column && index == other.index; }

	string_view column;
	int index = IndexValueType::NotSet;
};

struct SortExpressionJoinedIndex {
	SortExpressionJoinedIndex(size_t fieldInd, string_view c) : fieldIdx{fieldInd}, column{c}, index{IndexValueType::NotSet} {}
	double GetValue(IdType rowId, joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const SortExpressionJoinedIndex& other) const noexcept {
		return fieldIdx == other.fieldIdx && column == other.column && index == other.index;
	}

	size_t fieldIdx;
	string_view column;
	int index = IndexValueType::NotSet;
};

struct SortExpressionFuncRank {
	constexpr SortExpressionFuncRank() = default;
	constexpr bool operator==(const SortExpressionFuncRank&) const noexcept { return true; }
};

struct SortExpressionOperation {
	constexpr SortExpressionOperation(ArithmeticOpType _op = OpPlus, bool neg = false) : op(_op), negative(neg) {}
	bool operator==(const SortExpressionOperation& other) const noexcept { return op == other.op && negative == other.negative; }

	ArithmeticOpType op;
	bool negative;
};

class SortExpressionBracket : private Bracket {
public:
	SortExpressionBracket(size_t s, bool abs = false) : Bracket{s}, isAbs_{abs} {}
	using Bracket::Size;
	using Bracket::Append;
	using Bracket::Erase;
	bool IsAbs() const noexcept { return isAbs_; }
	void CopyPayloadFrom(const SortExpressionBracket& other) noexcept { isAbs_ = other.isAbs_; }
	bool operator==(const SortExpressionBracket& other) const noexcept { return Bracket::operator==(other) && isAbs_ == other.isAbs_; }

private:
	bool isAbs_ = false;
};

class SortExpression : public ExpressionTree<SortExpressionOperation, SortExpressionBracket, 2, SortExpressionValue, SortExpressionIndex,
											 SortExpressionJoinedIndex, SortExpressionFuncRank> {
public:
	template <typename T>
	static SortExpression Parse(string_view, const std::vector<T>& joinedSelectors);
	double Calculate(IdType rowId, ConstPayload pv, joins::NamespaceResults& results, const std::vector<JoinedSelector>& js, uint8_t proc,
					 TagsMatcher& tagsMatcher) const {
		return calculate(cbegin(), cend(), rowId, pv, results, js, proc, tagsMatcher);
	}
	bool ByIndexField() const;

	std::string Dump() const;

private:
	template <typename T>
	string_view::iterator parse(string_view::iterator begin, string_view::iterator end, bool* containIndexOrFunction, string_view fullExpr,
								const std::vector<T>& joinedSelectors);
	static double calculate(const_iterator begin, const_iterator end, IdType rowId, ConstPayload, joins::NamespaceResults&,
							const std::vector<JoinedSelector>&, uint8_t proc, TagsMatcher&);

	void openBracketBeforeLastAppended();
	static void dump(const_iterator begin, const_iterator end, WrSerializer&);
};
std::ostream& operator<<(std::ostream&, const SortExpression&);

}  // namespace reindexer
