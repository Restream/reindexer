#pragma once

#include <climits>
#include <string>
#include <vector>
#include "core/expressiontree.h"
#include "core/keyvalue/variant.h"
#include "core/payload/fieldsset.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "tools/serializer.h"
#include "tools/verifying_updater.h"

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
	std::string Dump(const std::vector<JS> &joinedSelectors) const;

	template <typename JS>
	std::string DumpOnCondition(const std::vector<JS> &joinedSelectors) const;
};

class QueryField {
public:
	using CompositeTypesVecT = h_vector<KeyValueType, 4>;

	template <typename Str>
	explicit QueryField(Str &&fieldName) noexcept : fieldName_{std::forward<Str>(fieldName)} {}
	QueryField(std::string &&fieldName, int idxNo, FieldsSet fields, KeyValueType fieldType,
			   std::vector<KeyValueType> &&compositeFieldsTypes);
	QueryField(QueryField &&) noexcept = default;
	QueryField(const QueryField &) = default;
	QueryField &operator=(QueryField &&) noexcept = default;

	[[nodiscard]] bool operator==(const QueryField &) const noexcept;
	[[nodiscard]] bool operator!=(const QueryField &other) const noexcept { return !operator==(other); }

	[[nodiscard]] int IndexNo() const noexcept { return idxNo_; }
	[[nodiscard]] bool IsFieldIndexed() const noexcept { return idxNo_ >= 0; }
	[[nodiscard]] bool FieldsHaveBeenSet() const noexcept { return idxNo_ != IndexValueType::NotSet; }
	[[nodiscard]] const FieldsSet &Fields() const & noexcept { return fieldsSet_; }
	[[nodiscard]] const std::string &FieldName() const & noexcept { return fieldName_; }
	[[nodiscard]] KeyValueType FieldType() const noexcept { return fieldType_; }
	[[nodiscard]] KeyValueType SelectType() const noexcept { return selectType_; }
	[[nodiscard]] const CompositeTypesVecT &CompositeFieldsTypes() const & noexcept { return compositeFieldsTypes_; }
	[[nodiscard]] bool HaveEmptyField() const noexcept;
	void SetField(FieldsSet &&fields) &;
	void SetIndexData(int idxNo, FieldsSet &&fields, KeyValueType fieldType, KeyValueType selectType,
					  CompositeTypesVecT &&compositeFieldsTypes) &;

	QueryField &operator=(const QueryField &) = delete;
	auto Fields() const && = delete;
	auto FieldName() const && = delete;
	auto CompositeFieldsTypes() const && = delete;

private:
	std::string fieldName_;
	int idxNo_{IndexValueType::NotSet};
	FieldsSet fieldsSet_;
	KeyValueType fieldType_{KeyValueType::Undefined{}};
	KeyValueType selectType_{KeyValueType::Undefined{}};
	CompositeTypesVecT compositeFieldsTypes_;
};

enum class VerifyQueryEntryFlags : unsigned { null = 0u, ignoreEmptyValues = 1u };
[[nodiscard]] RX_ALWAYS_INLINE constexpr bool operator&(VerifyQueryEntryFlags lhs, VerifyQueryEntryFlags rhs) noexcept {
	using UnderlyingType = std::underlying_type_t<VerifyQueryEntryFlags>;
	return static_cast<UnderlyingType>(lhs) & static_cast<UnderlyingType>(rhs);
}

template <VerifyQueryEntryFlags = VerifyQueryEntryFlags::null>
void VerifyQueryEntryValues(CondType, const VariantArray &);
extern template void VerifyQueryEntryValues<VerifyQueryEntryFlags::null>(CondType, const VariantArray &);
extern template void VerifyQueryEntryValues<VerifyQueryEntryFlags::ignoreEmptyValues>(CondType, const VariantArray &);

class QueryEntry : private QueryField {
public:
	enum : size_t { NotInjected = std::numeric_limits<size_t>::max(), InjectedFromMain = NotInjected - 1 };
	struct DistinctTag {};
	struct IgnoreEmptyValues {};
	static constexpr unsigned kDefaultLimit = UINT_MAX;
	static constexpr unsigned kDefaultOffset = 0;

	template <typename Str, typename VA,
			  std::enable_if_t<std::is_constructible_v<std::string, Str> && std::is_constructible_v<VariantArray, VA>> * = nullptr>
	QueryEntry(Str &&fieldName, CondType cond, VA &&v, size_t injectedFrom = NotInjected)
		: QueryField{std::forward<Str>(fieldName)}, values_{std::forward<VA>(v)}, condition_{cond}, injectedFrom_{injectedFrom} {
		Verify();
	}
	template <typename Str>
	QueryEntry(Str &&fieldName, DistinctTag) : QueryField(std::forward<Str>(fieldName)), condition_(CondAny), distinct_(true) {
		Verify();
	}
	template <typename VA, std::enable_if_t<std::is_constructible_v<VariantArray, VA>> * = nullptr>
	QueryEntry(QueryField &&field, CondType cond, VA &&v) : QueryField{std::move(field)}, values_{std::forward<VA>(v)}, condition_{cond} {
		Verify();
	}
	QueryEntry(QueryField &&field, CondType cond, IgnoreEmptyValues) : QueryField(std::move(field)), condition_(cond) {
		verifyIgnoringEmptyValues();
	}
	[[nodiscard]] CondType Condition() const noexcept { return condition_; }
	[[nodiscard]] const VariantArray &Values() const & noexcept { return values_; }
	[[nodiscard]] VariantArray &&Values() && noexcept { return std::move(values_); }
	[[nodiscard]] auto UpdatableValues(IgnoreEmptyValues) & noexcept {
		return VerifyingUpdater<QueryEntry, VariantArray, &QueryEntry::values_, &QueryEntry::verifyIgnoringEmptyValues>{*this};
	}
	[[nodiscard]] auto UpdatableValues() & noexcept {
		return VerifyingUpdater<QueryEntry, VariantArray, &QueryEntry::values_, &QueryEntry::verifyNotIgnoringEmptyValues>{*this};
	}
	[[nodiscard]] bool Distinct() const noexcept { return distinct_; }
	void Distinct(bool d) noexcept { distinct_ = d; }
	using QueryField::IndexNo;
	using QueryField::IsFieldIndexed;
	using QueryField::FieldsHaveBeenSet;
	using QueryField::Fields;
	using QueryField::FieldName;
	using QueryField::FieldType;
	using QueryField::SelectType;
	using QueryField::CompositeFieldsTypes;
	using QueryField::SetField;
	using QueryField::SetIndexData;
	using QueryField::HaveEmptyField;
	void SetCondAndValues(CondType cond, VariantArray &&values) {
		VerifyQueryEntryValues(cond, values);
		condition_ = cond;
		values_ = std::move(values);
	}

	const QueryField &FieldData() const & noexcept { return static_cast<const QueryField &>(*this); }
	QueryField &FieldData() & noexcept { return static_cast<QueryField &>(*this); }
	void ConvertValuesToFieldType() & {
		for (Variant &v : values_) {
			v.convert(SelectType());
		}
	}
	void ConvertValuesToFieldType(const PayloadType &pt) & {
		if (SelectType().Is<KeyValueType::Undefined>() || Condition() == CondDWithin) {
			return;
		}
		for (Variant &v : values_) {
			v.convert(SelectType(), &pt, &Fields());
		}
	}
	void Verify() const { VerifyQueryEntryValues(condition_, values_); }

	[[nodiscard]] bool operator==(const QueryEntry &) const noexcept;
	[[nodiscard]] bool operator!=(const QueryEntry &other) const noexcept { return !operator==(other); }

	[[nodiscard]] std::string Dump() const;
	[[nodiscard]] std::string DumpBrief() const;
	[[nodiscard]] bool IsInjectedFromMain() const noexcept { return injectedFrom_ == InjectedFromMain; }
	[[nodiscard]] bool IsInjectedFrom(size_t joinedQueryNo) const noexcept { return injectedFrom_ == joinedQueryNo; }
	void InjectedFrom(size_t joinedQueryNo) noexcept { injectedFrom_ = joinedQueryNo; }

	auto Values() const && = delete;
	auto FieldData() const && = delete;

private:
	void verifyIgnoringEmptyValues() const { VerifyQueryEntryValues<VerifyQueryEntryFlags::ignoreEmptyValues>(condition_, values_); }
	void verifyNotIgnoringEmptyValues() const { VerifyQueryEntryValues(condition_, values_); }

	VariantArray values_;
	CondType condition_;
	bool distinct_ = false;
	size_t injectedFrom_ = NotInjected;
};

class BetweenFieldsQueryEntry {
public:
	template <typename StrL, typename StrR>
	BetweenFieldsQueryEntry(StrL &&fstIdx, CondType cond, StrR &&sndIdx)
		: leftField_{std::forward<StrL>(fstIdx)}, rightField_{std::forward<StrR>(sndIdx)}, condition_{cond} {
		if (condition_ == CondAny || condition_ == CondEmpty || condition_ == CondDWithin) {
			throw Error{errLogic, "Condition '%s' is inapplicable between two fields", CondTypeToStr(condition_)};
		}
	}

	[[nodiscard]] bool operator==(const BetweenFieldsQueryEntry &) const noexcept;
	[[nodiscard]] bool operator!=(const BetweenFieldsQueryEntry &other) const noexcept { return !operator==(other); }

	[[nodiscard]] CondType Condition() const noexcept { return condition_; }
	[[nodiscard]] int LeftIdxNo() const noexcept { return leftField_.IndexNo(); }
	[[nodiscard]] int RightIdxNo() const noexcept { return rightField_.IndexNo(); }
	[[nodiscard]] const std::string &LeftFieldName() const & noexcept { return leftField_.FieldName(); }
	[[nodiscard]] const std::string &RightFieldName() const & noexcept { return rightField_.FieldName(); }
	[[nodiscard]] const FieldsSet &LeftFields() const & noexcept { return leftField_.Fields(); }
	[[nodiscard]] const FieldsSet &RightFields() const & noexcept { return rightField_.Fields(); }
	[[nodiscard]] KeyValueType LeftFieldType() const noexcept { return leftField_.FieldType(); }
	[[nodiscard]] KeyValueType RightFieldType() const noexcept { return rightField_.FieldType(); }
	[[nodiscard]] const QueryField::CompositeTypesVecT &LeftCompositeFieldsTypes() const & noexcept {
		return leftField_.CompositeFieldsTypes();
	}
	[[nodiscard]] const QueryField::CompositeTypesVecT &RightCompositeFieldsTypes() const & noexcept {
		return rightField_.CompositeFieldsTypes();
	}
	[[nodiscard]] const QueryField &LeftFieldData() const & noexcept { return leftField_; }
	[[nodiscard]] QueryField &LeftFieldData() & noexcept { return leftField_; }
	[[nodiscard]] const QueryField &RightFieldData() const & noexcept { return rightField_; }
	[[nodiscard]] QueryField &RightFieldData() & noexcept { return rightField_; }
	[[nodiscard]] bool FieldsHaveBeenSet() const noexcept { return leftField_.FieldsHaveBeenSet() && rightField_.FieldsHaveBeenSet(); }
	[[nodiscard]] bool IsLeftFieldIndexed() const noexcept { return leftField_.IsFieldIndexed(); }
	[[nodiscard]] bool IsRightFieldIndexed() const noexcept { return rightField_.IsFieldIndexed(); }
	[[nodiscard]] std::string Dump() const;

	auto LeftFieldName() const && = delete;
	auto RightFieldName() const && = delete;
	auto LeftFields() const && = delete;
	auto RightFields() const && = delete;
	auto LeftCompositeFieldsTypes() const && = delete;
	auto RightCompositeFieldsTypes() const && = delete;
	auto LeftFieldData() const && = delete;
	auto RightFieldData() const && = delete;

private:
	QueryField leftField_;
	QueryField rightField_;
	CondType condition_;
};

struct AlwaysFalse {};
constexpr bool operator==(AlwaysFalse, AlwaysFalse) noexcept { return true; }
struct AlwaysTrue {};
constexpr bool operator==(AlwaysTrue, AlwaysTrue) noexcept { return true; }

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

class SubQueryEntry {
public:
	SubQueryEntry(CondType cond, size_t qIdx, VariantArray &&values) : condition_{cond}, queryIndex_{qIdx}, values_{std::move(values)} {
		VerifyQueryEntryValues(condition_, values_);
	}
	[[nodiscard]] CondType Condition() const noexcept { return condition_; }
	[[nodiscard]] size_t QueryIndex() const noexcept { return queryIndex_; }
	[[nodiscard]] const VariantArray &Values() const & noexcept { return values_; }
	[[nodiscard]] bool operator==(const SubQueryEntry &other) const noexcept {
		return condition_ == other.condition_ && queryIndex_ == other.queryIndex_;
	}
	[[nodiscard]] bool operator!=(const SubQueryEntry &other) const noexcept { return !operator==(other); }
	[[nodiscard]] std::string Dump(const std::vector<Query> &subQueries) const;

	auto Values() const && = delete;

private:
	CondType condition_;
	// index of Query in Query::subQueries_
	size_t queryIndex_;
	VariantArray values_;
};

class SubQueryFieldEntry {
public:
	template <typename Str>
	SubQueryFieldEntry(Str &&field, CondType cond, size_t qIdx) : field_{std::forward<Str>(field)}, condition_{cond}, queryIndex_{qIdx} {
		if (cond == CondAny || cond == CondEmpty) {
			throw Error{errQueryExec, "Condition %s with field and subquery", cond == CondAny ? "Any" : "Empty"};
		}
	}
	[[nodiscard]] const std::string &FieldName() const & noexcept { return field_; }
	[[nodiscard]] std::string &&FieldName() && noexcept { return std::move(field_); }
	[[nodiscard]] CondType Condition() const noexcept { return condition_; }
	[[nodiscard]] size_t QueryIndex() const noexcept { return queryIndex_; }
	[[nodiscard]] bool operator==(const SubQueryFieldEntry &other) const noexcept {
		return field_ == other.field_ && condition_ == other.condition_ && queryIndex_ == other.queryIndex_;
	}
	[[nodiscard]] bool operator!=(const SubQueryFieldEntry &other) const noexcept { return !operator==(other); }
	[[nodiscard]] std::string Dump(const std::vector<Query> &subQueries) const;

	auto FieldName() const && = delete;

private:
	std::string field_;
	CondType condition_;
	// index of Query in Query::subQueries_
	size_t queryIndex_;
};

class UpdateEntry {
public:
	template <typename Str>
	UpdateEntry(Str &&c, VariantArray &&v, FieldModifyMode m = FieldModeSet, bool e = false)
		: column_(std::forward<Str>(c)), values_(std::move(v)), mode_(m), isExpression_(e) {
		if (column_.empty()) {
			throw Error{errParams, "Empty update column name"};
		}
	}
	bool operator==(const UpdateEntry &) const noexcept;
	bool operator!=(const UpdateEntry &obj) const noexcept { return !operator==(obj); }
	std::string_view Column() const noexcept { return column_; }
	VariantArray const &Values() const noexcept { return values_; }
	VariantArray &Values() noexcept { return values_; }
	FieldModifyMode Mode() const noexcept { return mode_; }
	void SetMode(FieldModifyMode m) noexcept { mode_ = m; }
	bool IsExpression() const noexcept { return isExpression_; }
	void SetIsExpression(bool e) noexcept { isExpression_ = e; }

private:
	std::string column_;
	VariantArray values_;
	FieldModifyMode mode_ = FieldModeSet;
	bool isExpression_ = false;
};

class QueryJoinEntry {
public:
	QueryJoinEntry(OpType op, CondType cond, std::string &&leftFld, std::string &&rightFld, bool reverseNs = false) noexcept
		: leftField_{std::move(leftFld)}, rightField_{std::move(rightFld)}, op_{op}, condition_{cond}, reverseNamespacesOrder_{reverseNs} {}
	[[nodiscard]] bool operator==(const QueryJoinEntry &) const noexcept;
	[[nodiscard]] bool operator!=(const QueryJoinEntry &other) const noexcept { return !operator==(other); }
	[[nodiscard]] bool IsLeftFieldIndexed() const noexcept { return leftField_.IsFieldIndexed(); }
	[[nodiscard]] bool IsRightFieldIndexed() const noexcept { return rightField_.IsFieldIndexed(); }
	[[nodiscard]] int LeftIdxNo() const noexcept { return leftField_.IndexNo(); }
	[[nodiscard]] int RightIdxNo() const noexcept { return rightField_.IndexNo(); }
	[[nodiscard]] const FieldsSet &LeftFields() const & noexcept { return leftField_.Fields(); }
	[[nodiscard]] const FieldsSet &RightFields() const & noexcept { return rightField_.Fields(); }
	[[nodiscard]] KeyValueType LeftFieldType() const noexcept { return leftField_.FieldType(); }
	[[nodiscard]] KeyValueType RightFieldType() const noexcept { return rightField_.FieldType(); }
	[[nodiscard]] const QueryField::CompositeTypesVecT &LeftCompositeFieldsTypes() const & noexcept {
		return leftField_.CompositeFieldsTypes();
	}
	[[nodiscard]] const QueryField::CompositeTypesVecT &RightCompositeFieldsTypes() const & noexcept {
		return rightField_.CompositeFieldsTypes();
	}
	[[nodiscard]] OpType Operation() const noexcept { return op_; }
	[[nodiscard]] CondType Condition() const noexcept { return condition_; }
	[[nodiscard]] const std::string &LeftFieldName() const & noexcept { return leftField_.FieldName(); }
	[[nodiscard]] const std::string &RightFieldName() const & noexcept { return rightField_.FieldName(); }
	[[nodiscard]] bool ReverseNamespacesOrder() const noexcept { return reverseNamespacesOrder_; }
	[[nodiscard]] const QueryField &LeftFieldData() const & noexcept { return leftField_; }
	[[nodiscard]] QueryField &LeftFieldData() & noexcept { return leftField_; }
	[[nodiscard]] const QueryField &RightFieldData() const & noexcept { return rightField_; }
	[[nodiscard]] QueryField &RightFieldData() & noexcept { return rightField_; }
	void SetLeftIndexData(int idxNo, FieldsSet &&fields, KeyValueType fieldType, KeyValueType selectType,
						  QueryField::CompositeTypesVecT &&compositeFieldsTypes) & {
		leftField_.SetIndexData(idxNo, std::move(fields), fieldType, selectType, std::move(compositeFieldsTypes));
	}
	void SetRightIndexData(int idxNo, FieldsSet &&fields, KeyValueType fieldType, KeyValueType selectType,
						   QueryField::CompositeTypesVecT &&compositeFieldsTypes) & {
		rightField_.SetIndexData(idxNo, std::move(fields), fieldType, selectType, std::move(compositeFieldsTypes));
	}
	void SetLeftField(FieldsSet &&fields) & { leftField_.SetField(std::move(fields)); }
	void SetRightField(FieldsSet &&fields) & { rightField_.SetField(std::move(fields)); }
	[[nodiscard]] bool FieldsHaveBeenSet() const noexcept { return leftField_.FieldsHaveBeenSet() && rightField_.FieldsHaveBeenSet(); }

	template <typename JS>
	std::string DumpCondition(const JS &joinedSelector, bool needOp = false) const;

	auto LeftFields() const && = delete;
	auto RightFields() const && = delete;
	auto LeftCompositeFieldsTypes() const && = delete;
	auto RightCompositeFieldsTypes() const && = delete;
	auto LeftFieldName() const && = delete;
	auto RightFieldName() const && = delete;
	auto LeftFieldData() const && = delete;
	auto RightFieldData() const && = delete;

private:
	QueryField leftField_;
	QueryField rightField_;
	const OpType op_;
	const CondType condition_;
	const bool reverseNamespacesOrder_;	 ///< controls SQL encoding order
										 ///< false: mainNs.index Condition joinNs.joinIndex
										 ///< true:  joinNs.joinIndex Invert(Condition) mainNs.index
};

enum class InjectionDirection : bool { IntoMain, FromMain };
class Index;

class QueryEntries : public ExpressionTree<OpType, QueryEntriesBracket, 4, QueryEntry, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse,
										   AlwaysTrue, SubQueryEntry, SubQueryFieldEntry> {
	using Base = ExpressionTree<OpType, QueryEntriesBracket, 4, QueryEntry, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse,
								AlwaysTrue, SubQueryEntry, SubQueryFieldEntry>;
	QueryEntries(Base &&b) : Base{std::move(b)} {}

public:
	QueryEntries() = default;
	QueryEntries(QueryEntries &&) = default;
	QueryEntries(const QueryEntries &) = default;
	QueryEntries &operator=(QueryEntries &&) = default;

	void ToDsl(const Query &parentQuery, JsonBuilder &builder) const { return toDsl(cbegin(), cend(), parentQuery, builder); }
	void WriteSQLWhere(const Query &parentQuery, WrSerializer &, bool stripArgs) const;
	void Serialize(WrSerializer &ser, const std::vector<Query> &subQueries) const { serialize(cbegin(), cend(), ser, subQueries); }
	bool CheckIfSatisfyConditions(const ConstPayload &pl) const { return checkIfSatisfyConditions(cbegin(), cend(), pl); }
	static bool CheckIfSatisfyCondition(const VariantArray &lValues, CondType, const VariantArray &rValues);
	template <InjectionDirection>
	[[nodiscard]] size_t InjectConditionsFromOnConditions(size_t position, const h_vector<QueryJoinEntry, 1> &joinEntries,
														  const QueryEntries &joinedQueryEntries, size_t joinedQueryNo,
														  const std::vector<std::unique_ptr<Index>> *indexesFrom);
	template <typename JS>
	std::string Dump(const std::vector<JS> &joinedSelectors, const std::vector<Query> &subQueries) const {
		WrSerializer ser;
		dump(0, cbegin(), cend(), joinedSelectors, subQueries, ser);
		dumpEqualPositions(0, ser, equalPositions);
		return std::string{ser.Slice()};
	}

	EqualPositions_t equalPositions;

private:
	static void toDsl(const_iterator it, const_iterator to, const Query &parentQuery, JsonBuilder &);
	static void writeSQL(const Query &parentQuery, const_iterator from, const_iterator to, WrSerializer &, bool stripArgs);
	static void serialize(const_iterator it, const_iterator to, WrSerializer &, const std::vector<Query> &subQueries);
	static void serialize(CondType, const VariantArray &values, WrSerializer &);
	static bool checkIfSatisfyConditions(const_iterator begin, const_iterator end, const ConstPayload &);
	static bool checkIfSatisfyCondition(const QueryEntry &, const ConstPayload &);
	static bool checkIfSatisfyCondition(const BetweenFieldsQueryEntry &, const ConstPayload &);
	[[nodiscard]] size_t injectConditionsFromOnCondition(size_t position, const std::string &fieldName, const std::string &joinedFieldName,
														 CondType, const QueryEntries &joinedQueryEntries, size_t injectedFrom,
														 size_t injectingInto, const std::vector<std::unique_ptr<Index>> *indexesFrom);

protected:
	static void dumpEqualPositions(size_t level, WrSerializer &, const EqualPositions_t &);
	template <typename JS>
	static void dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JS> &joinedSelectors,
					 const std::vector<Query> &subQueries, WrSerializer &);
};

extern template size_t QueryEntries::InjectConditionsFromOnConditions<InjectionDirection::FromMain>(
	size_t, const h_vector<QueryJoinEntry, 1> &, const QueryEntries &, size_t, const std::vector<std::unique_ptr<Index>> *);
extern template size_t QueryEntries::InjectConditionsFromOnConditions<InjectionDirection::IntoMain>(
	size_t, const h_vector<QueryJoinEntry, 1> &, const QueryEntries &, size_t, const std::vector<std::unique_ptr<Index>> *);

struct SortingEntry {
	SortingEntry() noexcept = default;
	template <typename Str>
	SortingEntry(Str &&e, bool d) noexcept : expression(std::forward<Str>(e)), desc(d) {}
	bool operator==(const SortingEntry &) const noexcept;
	bool operator!=(const SortingEntry &se) const noexcept { return !operator==(se); }
	std::string expression;
	bool desc = false;
	int index = IndexValueType::NotSet;
};

struct SortingEntries : public h_vector<SortingEntry, 1> {};

class AggregateEntry {
public:
	AggregateEntry(AggType type, h_vector<std::string, 1> &&fields, SortingEntries &&sort = {}, unsigned limit = QueryEntry::kDefaultLimit,
				   unsigned offset = QueryEntry::kDefaultOffset);
	[[nodiscard]] bool operator==(const AggregateEntry &) const noexcept;
	[[nodiscard]] bool operator!=(const AggregateEntry &ae) const noexcept { return !operator==(ae); }
	[[nodiscard]] AggType Type() const noexcept { return type_; }
	[[nodiscard]] const h_vector<std::string, 1> &Fields() const noexcept { return fields_; }
	[[nodiscard]] const SortingEntries &Sorting() const noexcept { return sortingEntries_; }
	[[nodiscard]] unsigned Limit() const noexcept { return limit_; }
	[[nodiscard]] unsigned Offset() const noexcept { return offset_; }
	void AddSortingEntry(SortingEntry &&);
	void SetLimit(unsigned);
	void SetOffset(unsigned);

private:
	AggType type_;
	h_vector<std::string, 1> fields_;
	SortingEntries sortingEntries_;
	unsigned limit_ = QueryEntry::kDefaultLimit;
	unsigned offset_ = QueryEntry::kDefaultOffset;
};

}  // namespace reindexer
