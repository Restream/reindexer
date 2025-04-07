#pragma once

#include "core/expressiontree.h"
#include "core/keyvalue/geometry.h"
#include "core/payload/payloadiface.h"
#include "core/rank_t.h"

namespace reindexer {

class ItemImpl;
class JoinedSelector;
class NamespaceImpl;

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
	Index(std::string c) : column{std::move(c)}, index{IndexValueType::NotSet} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const Index& other) const noexcept { return column == other.column && index == other.index; }

	std::string column;
	int index = IndexValueType::NotSet;
};

struct ProxiedField {
	ProxiedField(std::string j) : json{std::move(j)} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const ProxiedField& other) const noexcept { return json == other.json; }

	std::string json;
};

struct JoinedIndex {
	JoinedIndex(size_t nsInd, std::string c) : nsIdx{nsInd}, column{std::move(c)}, index{IndexValueType::NotSet} {}
	double GetValue(IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const JoinedIndex& other) const noexcept {
		return nsIdx == other.nsIdx && column == other.column && index == other.index;
	}

	size_t nsIdx;
	std::string column;
	int index = IndexValueType::NotSet;
};

struct Rank {
	constexpr Rank() = default;
	constexpr bool operator==(const Rank&) const noexcept { return true; }
};

struct DistanceFromPoint {
	DistanceFromPoint(std::string c, Point p) : column{std::move(c)}, index{IndexValueType::NotSet}, point{p} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const DistanceFromPoint& other) const noexcept {
		return column == other.column && index == other.index && point == other.point;
	}

	std::string column;
	int index = IndexValueType::NotSet;
	Point point;
};

struct ProxiedDistanceFromPoint {
	ProxiedDistanceFromPoint(std::string j, Point p) : json{std::move(j)}, point{p} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const ProxiedDistanceFromPoint& other) const noexcept { return json == other.json && point == other.point; }

	std::string json;
	Point point;
};

struct DistanceJoinedIndexFromPoint {
	DistanceJoinedIndexFromPoint(size_t nsInd, std::string c, Point p)
		: nsIdx{nsInd}, column{std::move(c)}, index{IndexValueType::NotSet}, point{p} {}
	double GetValue(IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const DistanceJoinedIndexFromPoint& other) const noexcept {
		return nsIdx == other.nsIdx && column == other.column && index == other.index && point == other.point;
	}

	size_t nsIdx;
	std::string column;
	int index = IndexValueType::NotSet;
	Point point;
};

struct DistanceBetweenIndexes {
	DistanceBetweenIndexes(std::string c1, std::string c2)
		: column1{std::move(c1)}, index1{IndexValueType::NotSet}, column2{std::move(c2)}, index2{IndexValueType::NotSet} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const DistanceBetweenIndexes& other) const noexcept {
		return column1 == other.column1 && index1 == other.index1 && column2 == other.column2 && index2 == other.index2;
	}

	std::string column1;
	int index1 = IndexValueType::NotSet;
	std::string column2;
	int index2 = IndexValueType::NotSet;
};

struct ProxiedDistanceBetweenFields {
	ProxiedDistanceBetweenFields(std::string j1, std::string j2) : json1{std::move(j1)}, json2{std::move(j2)} {}
	double GetValue(ConstPayload, TagsMatcher&) const;
	bool operator==(const ProxiedDistanceBetweenFields& other) const noexcept { return json1 == other.json1 && json2 == other.json2; }

	std::string json1;
	std::string json2;
};

struct DistanceBetweenIndexAndJoinedIndex {
	DistanceBetweenIndexAndJoinedIndex(std::string c, size_t jNsInd, std::string jc)
		: column{std::move(c)}, index{IndexValueType::NotSet}, jNsIdx{jNsInd}, jColumn{std::move(jc)}, jIndex{IndexValueType::NotSet} {}
	double GetValue(ConstPayload, TagsMatcher&, IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const DistanceBetweenIndexAndJoinedIndex& other) const noexcept {
		return column == other.column && index == other.index && jNsIdx == other.jNsIdx && jColumn == other.jColumn &&
			   jIndex == other.jIndex;
	}

	std::string column;
	int index = IndexValueType::NotSet;
	size_t jNsIdx;
	std::string jColumn;
	int jIndex = IndexValueType::NotSet;
};

struct DistanceBetweenJoinedIndexes {
	DistanceBetweenJoinedIndexes(size_t nsInd1, std::string c1, size_t nsInd2, std::string c2)
		: nsIdx1{nsInd1},
		  column1{std::move(c1)},
		  index1{IndexValueType::NotSet},
		  nsIdx2{nsInd2},
		  column2{std::move(c2)},
		  index2{IndexValueType::NotSet} {}
	double GetValue(IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const DistanceBetweenJoinedIndexes& other) const noexcept {
		return nsIdx1 == other.nsIdx1 && column1 == other.column1 && index1 == other.index1 && nsIdx2 == other.nsIdx2 &&
			   column2 == other.column2 && index2 == other.index2;
	}

	size_t nsIdx1;
	std::string column1;
	int index1 = IndexValueType::NotSet;
	size_t nsIdx2;
	std::string column2;
	int index2 = IndexValueType::NotSet;
};

struct DistanceBetweenJoinedIndexesSameNs {
	DistanceBetweenJoinedIndexesSameNs(size_t nsInd, std::string c1, std::string c2)
		: nsIdx{nsInd}, column1{std::move(c1)}, index1{IndexValueType::NotSet}, column2{std::move(c2)}, index2{IndexValueType::NotSet} {}
	double GetValue(IdType rowId, const joins::NamespaceResults&, const std::vector<JoinedSelector>&) const;
	bool operator==(const DistanceBetweenJoinedIndexesSameNs& other) const noexcept {
		return nsIdx == other.nsIdx && column1 == other.column1 && index1 == other.index1 && column2 == other.column2 &&
			   index2 == other.index2;
	}

	size_t nsIdx;
	std::string column1;
	int index1 = IndexValueType::NotSet;
	std::string column2;
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
	static SortExpression Parse(std::string_view, const std::vector<T>& joinedSelectors);
	[[nodiscard]] double Calculate(IdType rowId, ConstPayload pv, const joins::NamespaceResults* results,
								   const std::vector<JoinedSelector>& js, RankT proc, TagsMatcher& tagsMatcher) const {
		return calculate(cbegin(), cend(), rowId, pv, results, js, proc, tagsMatcher);
	}
	[[nodiscard]] bool ByField() const noexcept;
	[[nodiscard]] bool ByJoinedField() const noexcept;
	[[nodiscard]] SortExprFuncs::JoinedIndex& GetJoinedIndex() noexcept;
	void PrepareIndexes(const NamespaceImpl&);
	static void PrepareSortIndex(std::string& column, int& index, const NamespaceImpl&);

	[[nodiscard]] std::string Dump() const;
	[[nodiscard]] static VariantArray GetJoinedFieldValues(IdType rowId, const joins::NamespaceResults& joinResults,
														   const std::vector<JoinedSelector>&, size_t nsIdx, std::string_view column,
														   int index);

private:
	friend SortExprFuncs::JoinedIndex;
	friend SortExprFuncs::DistanceJoinedIndexFromPoint;
	friend SortExprFuncs::DistanceBetweenIndexAndJoinedIndex;
	friend SortExprFuncs::DistanceBetweenJoinedIndexes;
	friend SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;
	template <typename T>
	[[nodiscard]] std::string_view parse(std::string_view expr, bool* containIndexOrFunction, std::string_view fullExpr,
										 const std::vector<T>& joinedSelectors);
	template <typename T, typename SkipSW>
	void parseDistance(std::string_view& expr, const std::vector<T>& joinedSelectors, std::string_view fullExpr, ArithmeticOpType,
					   bool negative, const SkipSW& skipSpaces);
	[[nodiscard]] static double calculate(const_iterator begin, const_iterator end, IdType rowId, ConstPayload,
										  const joins::NamespaceResults*, const std::vector<JoinedSelector>&, RankT, TagsMatcher&);

	void openBracketBeforeLastAppended();
	static void dump(const_iterator begin, const_iterator end, WrSerializer&);
	[[nodiscard]] static const PayloadValue& getJoinedValue(IdType rowId, const joins::NamespaceResults& joinResults,
															const std::vector<JoinedSelector>&, size_t nsIdx);
};
std::ostream& operator<<(std::ostream&, const SortExpression&);

class ProxiedSortExpression
	: public ExpressionTree<SortExpressionOperation, SortExpressionBracket, 2, SortExprFuncs::Value, SortExprFuncs::ProxiedField,
							SortExprFuncs::Rank, SortExprFuncs::ProxiedDistanceFromPoint, SortExprFuncs::ProxiedDistanceBetweenFields> {
public:
	ProxiedSortExpression(const SortExpression& se, const NamespaceImpl& ns) { fill(se.cbegin(), se.cend(), ns); }
	double Calculate(IdType rowId, ConstPayload pv, RankT rank, TagsMatcher& tagsMatcher) const {
		return calculate(cbegin(), cend(), rowId, pv, rank, tagsMatcher);
	}
	std::string Dump() const;

private:
	void fill(SortExpression::const_iterator begin, SortExpression::const_iterator end, const NamespaceImpl&);
	static std::string getJsonPath(std::string_view columnName, int idx, const NamespaceImpl&);
	static double calculate(const_iterator begin, const_iterator end, IdType rowId, ConstPayload, RankT, TagsMatcher&);
	static void dump(const_iterator begin, const_iterator end, WrSerializer&);
};
std::ostream& operator<<(std::ostream&, const ProxiedSortExpression&);

}  // namespace reindexer
