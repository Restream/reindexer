#pragma once

#include "core/expressiontree.h"
#include "core/keyvalue/geometry.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

class Index;
class ItemImpl;
class JoinedSelector;

namespace joins {
class NamespaceResults;
}  // namespace joins

namespace SortExprFuncs {

struct Value {
	Value(double v) : value{v} {}
	bool operator==(const Value& other) const noexcept { return value == other.value; }

	double value;
};

struct Index {
	Index(string_view c) : column{c}, index{IndexValueType::NotSet} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const Index& other) const noexcept { return column == other.column && index == other.index; }

	string_view column;
	int index = IndexValueType::NotSet;
};

struct JoinedIndex {
	JoinedIndex(size_t nsInd, string_view c) : nsIdx{nsInd}, column{c}, index{IndexValueType::NotSet} {}
	double GetValue(IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const JoinedIndex& other) const noexcept {
		return nsIdx == other.nsIdx && column == other.column && index == other.index;
	}

	size_t nsIdx;
	string_view column;
	int index = IndexValueType::NotSet;
};

struct Rank {
	constexpr Rank() = default;
	constexpr bool operator==(const Rank&) const noexcept { return true; }
};

struct DistanceFromPoint {
	DistanceFromPoint(string_view c, Point p) : column{c}, index{IndexValueType::NotSet}, point{p} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const DistanceFromPoint& other) const noexcept {
		return column == other.column && index == other.index && point == other.point;
	}

	string_view column;
	int index = IndexValueType::NotSet;
	Point point;
};

struct DistanceJoinedIndexFromPoint {
	DistanceJoinedIndexFromPoint(size_t nsInd, string_view c, Point p) : nsIdx{nsInd}, column{c}, index{IndexValueType::NotSet}, point{p} {}
	double GetValue(IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const DistanceJoinedIndexFromPoint& other) const noexcept {
		return nsIdx == other.nsIdx && column == other.column && index == other.index && point == other.point;
	}

	size_t nsIdx;
	string_view column;
	int index = IndexValueType::NotSet;
	Point point;
};

struct DistanceBetweenIndexes {
	DistanceBetweenIndexes(string_view c1, string_view c2)
		: column1{c1}, index1{IndexValueType::NotSet}, column2{c2}, index2{IndexValueType::NotSet} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const DistanceBetweenIndexes& other) const noexcept {
		return column1 == other.column1 && index1 == other.index1 && column2 == other.column2 && index2 == other.index2;
	}

	string_view column1;
	int index1 = IndexValueType::NotSet;
	string_view column2;
	int index2 = IndexValueType::NotSet;
};

struct DistanceBetweenIndexAndJoinedIndex {
	DistanceBetweenIndexAndJoinedIndex(string_view c, size_t jNsInd, string_view jc)
		: column{c}, index{IndexValueType::NotSet}, jNsIdx{jNsInd}, jColumn{jc}, jIndex{IndexValueType::NotSet} {}
	double GetValue(ConstPayload, TagsMatcher&, IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const DistanceBetweenIndexAndJoinedIndex& other) const noexcept {
		return column == other.column && index == other.index && jNsIdx == other.jNsIdx && jColumn == other.jColumn &&
			   jIndex == other.jIndex;
	}

	string_view column;
	int index = IndexValueType::NotSet;
	size_t jNsIdx;
	string_view jColumn;
	int jIndex = IndexValueType::NotSet;
};

struct DistanceBetweenJoinedIndexes {
	DistanceBetweenJoinedIndexes(size_t nsInd1, string_view c1, size_t nsInd2, string_view c2)
		: nsIdx1{nsInd1}, column1{c1}, index1{IndexValueType::NotSet}, nsIdx2{nsInd2}, column2{c2}, index2{IndexValueType::NotSet} {}
	double GetValue(IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const DistanceBetweenJoinedIndexes& other) const noexcept {
		return nsIdx1 == other.nsIdx1 && column1 == other.column1 && index1 == other.index1 && nsIdx2 == other.nsIdx2 &&
			   column2 == other.column2 && index2 == other.index2;
	}

	size_t nsIdx1;
	string_view column1;
	int index1 = IndexValueType::NotSet;
	size_t nsIdx2;
	string_view column2;
	int index2 = IndexValueType::NotSet;
};

struct DistanceBetweenJoinedIndexesSameNs {
	DistanceBetweenJoinedIndexesSameNs(size_t nsInd, string_view c1, string_view c2)
		: nsIdx{nsInd}, column1{c1}, index1{IndexValueType::NotSet}, column2{c2}, index2{IndexValueType::NotSet} {}
	double GetValue(IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const DistanceBetweenJoinedIndexesSameNs& other) const noexcept {
		return nsIdx == other.nsIdx && column1 == other.column1 && index1 == other.index1 && column2 == other.column2 &&
			   index2 == other.index2;
	}

	size_t nsIdx;
	string_view column1;
	int index1 = IndexValueType::NotSet;
	string_view column2;
	int index2 = IndexValueType::NotSet;
};

}  // namespace SortExprFuncs

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

class SortExpression : public ExpressionTree<SortExpressionOperation, SortExpressionBracket, 2, SortExprFuncs::Value, SortExprFuncs::Index,
											 SortExprFuncs::JoinedIndex, SortExprFuncs::Rank, SortExprFuncs::DistanceFromPoint,
											 SortExprFuncs::DistanceJoinedIndexFromPoint, SortExprFuncs::DistanceBetweenIndexes,
											 SortExprFuncs::DistanceBetweenIndexAndJoinedIndex, SortExprFuncs::DistanceBetweenJoinedIndexes,
											 SortExprFuncs::DistanceBetweenJoinedIndexesSameNs> {
public:
	template <typename T>
	static SortExpression Parse(string_view, const std::vector<T>& joinedSelectors);
	double Calculate(IdType rowId, ConstPayload pv, const joins::NamespaceResults& results, const std::vector<JoinedSelector>& js,
					 uint8_t proc, TagsMatcher& tagsMatcher) const {
		return calculate(cbegin(), cend(), rowId, pv, results, js, proc, tagsMatcher);
	}
	bool ByIndexField() const;
	bool ByJoinedIndexField() const;

	std::string Dump() const;

private:
	friend SortExprFuncs::JoinedIndex;
	friend SortExprFuncs::DistanceJoinedIndexFromPoint;
	friend SortExprFuncs::DistanceBetweenIndexAndJoinedIndex;
	friend SortExprFuncs::DistanceBetweenJoinedIndexes;
	friend SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;
	template <typename T>
	string_view::iterator parse(string_view::iterator begin, string_view::iterator end, bool* containIndexOrFunction, string_view fullExpr,
								const std::vector<T>& joinedSelectors);
	template <typename T, typename SkipSW>
	void parseDistance(string_view::iterator& it, string_view::iterator end, const std::vector<T>& joinedSelectors, string_view fullExpr,
					   ArithmeticOpType, bool negative, const SkipSW& skipSpaces);
	static double calculate(const_iterator begin, const_iterator end, IdType rowId, ConstPayload, const joins::NamespaceResults&,
							const std::vector<JoinedSelector>&, uint8_t proc, TagsMatcher&);

	void openBracketBeforeLastAppended();
	static void dump(const_iterator begin, const_iterator end, WrSerializer&);
	static ItemImpl getJoinedItem(IdType rowId, const joins::NamespaceResults& joinResults, const std::vector<JoinedSelector>&,
								  size_t nsIdx);
	static VariantArray getJoinedFieldValues(IdType rowId, const joins::NamespaceResults& joinResults, const std::vector<JoinedSelector>&,
											 size_t nsIdx, string_view column, int index);
};
std::ostream& operator<<(std::ostream&, const SortExpression&);

}  // namespace reindexer
