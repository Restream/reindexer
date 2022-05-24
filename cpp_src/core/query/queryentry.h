#pragma once

#include <climits>
#include <string>
#include <vector>
#include "core/expressiontree.h"
#include "core/keyvalue/variant.h"
#include "core/type_consts_helpers.h"
#include "estl/h_vector.h"
#include "tools/serializer.h"

namespace reindexer {

class Query;
template <typename T>
class PayloadIface;
using ConstPayload = PayloadIface<const PayloadValue>;
class TagsMatcher;

struct JoinQueryEntry {
	JoinQueryEntry(size_t joinIdx) noexcept : joinIndex{joinIdx} {}
	size_t joinIndex;
	bool operator==(const JoinQueryEntry &other) const noexcept { return joinIndex == other.joinIndex; }
	bool operator!=(const JoinQueryEntry &other) const noexcept { return !operator==(other); }

	template <typename JS>
	std::string Dump(const std::vector<JS> &joinedSelectors) const {
		WrSerializer ser;
		const auto &js = joinedSelectors.at(joinIndex);
		const auto &q = js.JoinQuery();
		ser << js.Type() << " (" << q.GetSQL() << ") ON ";
		ser << '(';
		for (const auto &jqe : q.joinEntries_) {
			if (&jqe != &q.joinEntries_.front()) {
				ser << ' ' << jqe.op_ << ' ';
			} else {
				assertrx(jqe.op_ == OpAnd);
			}
			ser << q._namespace << '.' << jqe.joinIndex_ << ' ' << InvertJoinCondition(jqe.condition_) << ' ' << jqe.index_;
		}
		ser << ')';
		return std::string{ser.Slice()};
	}
};

struct QueryEntry {
	QueryEntry(std::string idx, CondType cond, VariantArray v) : index{std::move(idx)}, condition{cond}, values(std::move(v)) {}
	QueryEntry(CondType cond, std::string idx, int idxN, bool dist = false)
		: index(std::move(idx)), idxNo(idxN), condition(cond), distinct(dist) {}
	QueryEntry() = default;

	bool operator==(const QueryEntry &) const;
	bool operator!=(const QueryEntry &other) const { return !operator==(other); }

	std::string index;
	int idxNo = IndexValueType::NotSet;
	CondType condition = CondType::CondAny;
	bool distinct = false;
	VariantArray values;

	std::string Dump() const;
};

class BetweenFieldsQueryEntry {
public:
	BetweenFieldsQueryEntry(std::string fstIdx, CondType cond, std::string sndIdx);

	bool operator==(const BetweenFieldsQueryEntry &) const noexcept;
	bool operator!=(const BetweenFieldsQueryEntry &other) const noexcept { return !operator==(other); }

	std::string firstIndex;
	std::string secondIndex;
	int firstIdxNo = IndexValueType::NotSet;
	int secondIdxNo = IndexValueType::NotSet;

	CondType Condition() const noexcept { return condition_; }
	std::string Dump() const;

private:
	CondType condition_;
};

struct AlwaysFalse {};
constexpr bool operator==(AlwaysFalse, AlwaysFalse) noexcept { return true; }

class JsonBuilder;

using EqualPosition_t = h_vector<std::string, 2>;
using EqualPositions_t = std::vector<EqualPosition_t>;

struct QueryEntriesBracket : public Bracket {
	using Bracket::Bracket;
	bool operator==(const QueryEntriesBracket &other) const noexcept {
		return Bracket::operator==(other) && equalPositions == other.equalPositions;
	}
	EqualPositions_t equalPositions;
};

class QueryEntries
	: public ExpressionTree<OpType, QueryEntriesBracket, 4, QueryEntry, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse> {
	using Base = ExpressionTree<OpType, QueryEntriesBracket, 4, QueryEntry, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse>;
	QueryEntries(Base &&b) : Base{std::move(b)} {}

public:
	QueryEntries() = default;
	QueryEntries(QueryEntries &&) = default;
	QueryEntries(const QueryEntries &) = default;
	QueryEntries &operator=(QueryEntries &&) = default;
	QueryEntries MakeLazyCopy() & { return {makeLazyCopy()}; }

	void ToDsl(const Query &parentQuery, JsonBuilder &builder) const { return toDsl(cbegin(), cend(), parentQuery, builder); }
	void WriteSQLWhere(const Query &parentQuery, WrSerializer &, bool stripArgs) const;
	void Serialize(WrSerializer &ser) const { serialize(cbegin(), cend(), ser); }
	bool CheckIfSatisfyConditions(const ConstPayload &pl, TagsMatcher &tm) const {
		return checkIfSatisfyConditions(cbegin(), cend(), pl, tm);
	}
	template <typename JS>
	std::string Dump(const std::vector<JS> &joinedSelectors) const {
		WrSerializer ser;
		dump(0, cbegin(), cend(), joinedSelectors, ser);
		return std::string{ser.Slice()};
	}

	EqualPositions_t equalPositions;

private:
	static void toDsl(const_iterator it, const_iterator to, const Query &parentQuery, JsonBuilder &);
	static void writeSQL(const Query &parentQuery, const_iterator from, const_iterator to, WrSerializer &, bool stripArgs);
	static void serialize(const_iterator it, const_iterator to, WrSerializer &);
	static bool checkIfSatisfyConditions(const_iterator begin, const_iterator end, const ConstPayload &, TagsMatcher &);
	static bool checkIfSatisfyCondition(const QueryEntry &, const ConstPayload &, TagsMatcher &);
	static bool checkIfSatisfyCondition(const BetweenFieldsQueryEntry &, const ConstPayload &, TagsMatcher &);
	static bool checkIfSatisfyCondition(const VariantArray &lValues, CondType, const VariantArray &rValues);
	template <typename JS>
	static void dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JS> &joinedSelectors, WrSerializer &ser) {
		for (const_iterator it = begin; it != end; ++it) {
			for (size_t i = 0; i < level; ++i) {
				ser << "   ";
			}
			if (it != begin || it->operation != OpAnd) {
				ser << it->operation << ' ';
			}
			it->InvokeAppropriate<void>(
				[&](const QueryEntriesBracket &) {
					ser << "(\n";
					dump(level + 1, it.cbegin(), it.cend(), joinedSelectors, ser);
					for (size_t i = 0; i < level; ++i) {
						ser << "   ";
					}
					ser << ")\n";
				},
				[&ser](const QueryEntry &qe) { ser << qe.Dump() << '\n'; },
				[&joinedSelectors, &ser](const JoinQueryEntry &jqe) { ser << jqe.Dump(joinedSelectors) << '\n'; },
				[&ser](const BetweenFieldsQueryEntry &qe) { ser << qe.Dump() << '\n'; },
				[&ser](const AlwaysFalse &) { ser << "AlwaysFalse" << 'n'; });
		}
	}
};

struct UpdateEntry {
	UpdateEntry() {}
	UpdateEntry(std::string c, VariantArray v, FieldModifyMode m = FieldModeSet, bool e = false)
		: column(std::move(c)), values(std::move(v)), mode(m), isExpression(e) {}
	bool operator==(const UpdateEntry &) const noexcept;
	bool operator!=(const UpdateEntry &obj) const noexcept { return !operator==(obj); }
	std::string column;
	VariantArray values;
	FieldModifyMode mode = FieldModeSet;
	bool isExpression = false;
	bool isArray = false;
};

struct QueryJoinEntry {
	QueryJoinEntry() = default;
	QueryJoinEntry(OpType op, CondType cond, std::string idx, std::string jIdx)
		: op_{op}, condition_{cond}, index_{std::move(idx)}, joinIndex_{std::move(jIdx)} {}
	bool operator==(const QueryJoinEntry &) const noexcept;
	bool operator!=(const QueryJoinEntry &qje) const noexcept { return !operator==(qje); }
	OpType op_ = OpAnd;
	CondType condition_ = CondEq;
	std::string index_;
	std::string joinIndex_;
	int idxNo = -1;
	bool reverseNamespacesOrder = false;
};

struct SortingEntry {
	SortingEntry() {}
	SortingEntry(const std::string &e, bool d) : expression(e), desc(d) {}
	bool operator==(const SortingEntry &) const noexcept;
	bool operator!=(const SortingEntry &se) const noexcept { return !operator==(se); }
	std::string expression;
	bool desc = false;
	int index = IndexValueType::NotSet;
};

struct SortingEntries : public h_vector<SortingEntry, 1> {};

struct AggregateEntry {
	AggregateEntry() = default;
	AggregateEntry(AggType type, const h_vector<std::string, 1> &fields, unsigned limit = UINT_MAX, unsigned offset = 0)
		: type_(type), fields_(fields), limit_(limit), offset_(offset) {}
	bool operator==(const AggregateEntry &) const noexcept;
	bool operator!=(const AggregateEntry &ae) const noexcept { return !operator==(ae); }
	AggType type_;
	h_vector<std::string, 1> fields_;
	SortingEntries sortingEntries_;
	unsigned limit_ = UINT_MAX;
	unsigned offset_ = 0;
};

}  // namespace reindexer
