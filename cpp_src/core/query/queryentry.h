#pragma once

#include <climits>
#include <string>
#include <vector>
#include "core/expressiontree.h"
#include "core/keyvalue/variant.h"
#include "core/payload/fieldsset.h"
#include "core/query/knn_search_params.h"
#include "core/type_consts.h"
#include "estl/concepts.h"
#include "estl/h_vector.h"
#include "tools/serializer.h"
#include "tools/verifying_updater.h"

namespace reindexer {

class Query;

namespace builders {
class JsonBuilder;
}  // namespace builders
using builders::JsonBuilder;

template <typename T>
class PayloadIface;
using ConstPayload = PayloadIface<const PayloadValue>;
class TagsMatcher;

struct [[nodiscard]] JoinQueryEntry {
	explicit JoinQueryEntry(size_t joinIdx) noexcept : joinIndex{joinIdx} {}

	bool operator==(const JoinQueryEntry& other) const noexcept = default;
	bool operator!=(const JoinQueryEntry& other) const noexcept = default;

	template <typename JS>
	std::string Dump(const std::vector<JS>& joinedSelectors) const;

	template <typename JS>
	std::string DumpOnCondition(const std::vector<JS>& joinedSelectors) const;

	size_t joinIndex{std::numeric_limits<size_t>::max()};
};

class [[nodiscard]] QueryField {
public:
	using CompositeTypesVecT = h_vector<KeyValueType, 4>;

	template <concepts::ConvertibleToString Str>
	explicit QueryField(Str&& fieldName) noexcept : fieldName_{std::forward<Str>(fieldName)} {}
	QueryField(QueryField&&) noexcept = default;
	QueryField(const QueryField&) = default;
	QueryField& operator=(QueryField&&) noexcept = default;

	bool operator==(const QueryField&) const noexcept;
	bool operator!=(const QueryField& other) const noexcept { return !operator==(other); }

	int IndexNo() const noexcept { return idxNo_; }
	bool IsFieldIndexed() const noexcept { return idxNo_ >= 0; }
	bool FieldsHaveBeenSet() const noexcept { return idxNo_ != IndexValueType::NotSet; }
	const FieldsSet& Fields() const& noexcept { return fieldsSet_; }
	const std::string& FieldName() const& noexcept { return fieldName_; }
	KeyValueType FieldType() const noexcept { return fieldType_; }
	KeyValueType SelectType() const noexcept { return selectType_; }
	const CompositeTypesVecT& CompositeFieldsTypes() const& noexcept { return compositeFieldsTypes_; }
	bool HaveEmptyField() const noexcept;
	void SetField(FieldsSet&& fields) &;
	void SetIndexData(int idxNo, std::string_view idxName, FieldsSet&& fields, KeyValueType fieldType, KeyValueType selectType,
					  CompositeTypesVecT&& compositeFieldsTypes) &;

	QueryField& operator=(const QueryField&) = delete;
	auto Fields() const&& = delete;
	auto FieldName() const&& = delete;
	auto CompositeFieldsTypes() const&& = delete;

private:
	std::string fieldName_;
	int idxNo_{IndexValueType::NotSet};
	FieldsSet fieldsSet_;
	KeyValueType fieldType_{KeyValueType::Undefined{}};
	KeyValueType selectType_{KeyValueType::Undefined{}};
	CompositeTypesVecT compositeFieldsTypes_;
};

enum class [[nodiscard]] VerifyQueryEntryFlags : unsigned { null = 0u, ignoreEmptyValues = 1u };
RX_ALWAYS_INLINE constexpr bool operator&(VerifyQueryEntryFlags lhs, VerifyQueryEntryFlags rhs) noexcept {
	using UnderlyingType = std::underlying_type_t<VerifyQueryEntryFlags>;
	return static_cast<UnderlyingType>(lhs) & static_cast<UnderlyingType>(rhs);
}

template <VerifyQueryEntryFlags = VerifyQueryEntryFlags::null>
void VerifyQueryEntryValues(CondType, const VariantArray&);
extern template void VerifyQueryEntryValues<VerifyQueryEntryFlags::null>(CondType, const VariantArray&);
extern template void VerifyQueryEntryValues<VerifyQueryEntryFlags::ignoreEmptyValues>(CondType, const VariantArray&);

class [[nodiscard]] QueryEntry : private QueryField {
public:
	enum [[nodiscard]] : size_t { NotInjected = std::numeric_limits<size_t>::max(), InjectedFromMain = NotInjected - 1 };
	struct [[nodiscard]] DistinctTag {};
	struct [[nodiscard]] ForcedSortOptEntryTag {};
	struct [[nodiscard]] IgnoreEmptyValues {};
	static constexpr unsigned kDefaultLimit = UINT_MAX;
	static constexpr unsigned kDefaultOffset = 0;

	template <concepts::ConvertibleToString Str, concepts::ConvertibleToVariantArray VA>
	QueryEntry(Str&& fieldName, CondType cond, VA&& v, size_t injectedFrom = NotInjected)
		: QueryField{std::forward<Str>(fieldName)}, values_{std::forward<VA>(v)}, condition_{cond}, injectedFrom_{injectedFrom} {
		std::ignore = adjust(AdjustMode::Default);
		Verify();
		ommitExcesiveArguments();
	}
	template <concepts::ConvertibleToString Str>
	QueryEntry(Str&& fieldName, DistinctTag) : QueryField(std::forward<Str>(fieldName)), condition_(CondAny), distinct_(IsDistinct_True) {
		std::ignore = adjust(AdjustMode::Default);
		Verify();
		ommitExcesiveArguments();
	}
	template <concepts::ConvertibleToVariantArray VA>
	QueryEntry(QueryField&& field, CondType cond, VA&& v) : QueryField{std::move(field)}, values_{std::forward<VA>(v)}, condition_{cond} {
		std::ignore = adjust(AdjustMode::Default);
		Verify();
		ommitExcesiveArguments();
	}
	template <concepts::ConvertibleToVariantArray VA>
	QueryEntry(QueryField&& field, CondType cond, VA&& v, ForcedSortOptEntryTag)
		: QueryField{std::move(field)}, values_{std::forward<VA>(v)}, condition_{cond}, forcedSortOptEntry_{IsForcedSortOptEntry_True} {
		std::ignore = adjust(AdjustMode::Default);
		Verify();
		ommitExcesiveArguments();
	}
	QueryEntry(QueryField&& field, CondType cond, IgnoreEmptyValues) : QueryField(std::move(field)), condition_(cond) {
		std::ignore = adjust(AdjustMode::Default);
		VerifyQueryEntryValues<VerifyQueryEntryFlags::ignoreEmptyValues>(condition_, values_);
		ommitExcesiveArguments();
	}
	template <concepts::ConvertibleToVariantArray VA>
	QueryEntry(const QueryEntry& qe, CondType cond, VA&& v) : QueryField{qe}, values_{std::forward<VA>(v)}, condition_(cond) {
		std::ignore = adjust(AdjustMode::Default);
		Verify();
		ommitExcesiveArguments();
	}
	CondType Condition() const noexcept { return condition_; }
	const VariantArray& Values() const& noexcept { return values_; }
	VariantArray&& Values() && noexcept { return std::move(values_); }
	auto UpdatableValues() & noexcept { return VerifyingUpdater<QueryEntry, VariantArray, &QueryEntry::values_>{*this}; }
	IsDistinct Distinct() const noexcept { return distinct_; }
	IsForcedSortOptEntry ForcedSortOptEntry() const noexcept { return forcedSortOptEntry_; }
	void Distinct(IsDistinct d) noexcept { distinct_ = d; }
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
	void SetCondAndValues(CondType cond, VariantArray&& values) {
		VerifyQueryEntryValues(cond, values);
		condition_ = cond;
		values_ = std::move(values);
	}

	const QueryField& FieldData() const& noexcept { return static_cast<const QueryField&>(*this); }
	QueryField& FieldData() & noexcept { return static_cast<QueryField&>(*this); }
	void ConvertValuesToFieldType() & {
		for (Variant& v : values_) {
			std::ignore = v.convert(SelectType());
		}
	}
	void ConvertValuesToFieldType(const PayloadType& pt) & {
		if (SelectType().Is<KeyValueType::Undefined>() || Condition() == CondDWithin) {
			return;
		}
		for (Variant& v : values_) {
			std::ignore = v.convert(SelectType(), &pt, &Fields());
		}
	}
	void Verify() const { VerifyQueryEntryValues(condition_, values_); }

	bool operator==(const QueryEntry&) const;
	bool operator!=(const QueryEntry& other) const { return !operator==(other); }

	std::string Dump() const;
	std::string DumpBrief() const;
	bool IsInjectedFrom(size_t joinedQueryNo) const noexcept { return injectedFrom_ == joinedQueryNo; }
	bool NeedIsNull() const noexcept { return needIsNull_; }
	void ResetNeedIsNull() noexcept { needIsNull_ = false; }
	bool TryUpdateInplace(VariantArray& newValues) noexcept;
	bool IsDistinctOnly() const noexcept { return distinct_ && (condition_ == CondAny); }

	auto Values() const&& = delete;
	auto FieldData() const&& = delete;

private:
	enum class [[nodiscard]] AdjustMode { Default, DryRun };

	bool adjust(AdjustMode mode) noexcept;
	void ommitExcesiveArguments() {
		if (condition_ == CondAny || condition_ == CondEmpty) {
			values_.Clear();
		}
	}

	VariantArray values_;
	CondType condition_{CondAny};
	size_t injectedFrom_{NotInjected};
	IsDistinct distinct_{IsDistinct_False};
	IsForcedSortOptEntry forcedSortOptEntry_{IsForcedSortOptEntry_False};
	bool needIsNull_{false};
};

class [[nodiscard]] BetweenFieldsQueryEntry {
public:
	template <typename StrL, typename StrR>
	BetweenFieldsQueryEntry(StrL&& fstIdx, CondType cond, StrR&& sndIdx)
		: leftField_{std::forward<StrL>(fstIdx)}, rightField_{std::forward<StrR>(sndIdx)}, condition_{cond} {
		checkCondition(cond);
	}

	bool operator==(const BetweenFieldsQueryEntry&) const noexcept = default;
	bool operator!=(const BetweenFieldsQueryEntry& other) const noexcept = default;

	CondType Condition() const noexcept { return condition_; }
	int LeftIdxNo() const noexcept { return leftField_.IndexNo(); }
	int RightIdxNo() const noexcept { return rightField_.IndexNo(); }
	const std::string& LeftFieldName() const& noexcept { return leftField_.FieldName(); }
	const std::string& RightFieldName() const& noexcept { return rightField_.FieldName(); }
	const FieldsSet& LeftFields() const& noexcept { return leftField_.Fields(); }
	const FieldsSet& RightFields() const& noexcept { return rightField_.Fields(); }
	KeyValueType LeftFieldType() const noexcept { return leftField_.FieldType(); }
	KeyValueType RightFieldType() const noexcept { return rightField_.FieldType(); }
	const QueryField::CompositeTypesVecT& LeftCompositeFieldsTypes() const& noexcept { return leftField_.CompositeFieldsTypes(); }
	const QueryField::CompositeTypesVecT& RightCompositeFieldsTypes() const& noexcept { return rightField_.CompositeFieldsTypes(); }
	const QueryField& LeftFieldData() const& noexcept { return leftField_; }
	QueryField& LeftFieldData() & noexcept { return leftField_; }
	const QueryField& RightFieldData() const& noexcept { return rightField_; }
	QueryField& RightFieldData() & noexcept { return rightField_; }
	bool FieldsHaveBeenSet() const noexcept { return leftField_.FieldsHaveBeenSet() && rightField_.FieldsHaveBeenSet(); }
	bool IsLeftFieldIndexed() const noexcept { return leftField_.IsFieldIndexed(); }
	bool IsRightFieldIndexed() const noexcept { return rightField_.IsFieldIndexed(); }
	std::string Dump() const;

	auto LeftFieldName() const&& = delete;
	auto RightFieldName() const&& = delete;
	auto LeftFields() const&& = delete;
	auto RightFields() const&& = delete;
	auto LeftCompositeFieldsTypes() const&& = delete;
	auto RightCompositeFieldsTypes() const&& = delete;
	auto LeftFieldData() const&& = delete;
	auto RightFieldData() const&& = delete;

private:
	void checkCondition(CondType cond) const;

	QueryField leftField_;
	QueryField rightField_;
	CondType condition_{CondAny};
};

struct [[nodiscard]] AlwaysFalse {};
constexpr bool operator==(AlwaysFalse, AlwaysFalse) noexcept { return true; }
struct [[nodiscard]] AlwaysTrue {};
constexpr bool operator==(AlwaysTrue, AlwaysTrue) noexcept { return true; }

using EqualPosition_t = h_vector<std::string, 2>;
using EqualPositions_t = std::vector<EqualPosition_t>;

struct [[nodiscard]] QueryEntriesBracket : public Bracket {
	using Bracket::Bracket;
	bool operator==(const QueryEntriesBracket& other) const noexcept {
		return Bracket::operator==(other) && equalPositions == other.equalPositions;
	}
	EqualPositions_t equalPositions;
};

class [[nodiscard]] SubQueryEntry {
public:
	SubQueryEntry(CondType cond, size_t qIdx, VariantArray&& values) : condition_{cond}, queryIndex_{qIdx}, values_{std::move(values)} {
		VerifyQueryEntryValues(condition_, values_);
	}
	CondType Condition() const noexcept { return condition_; }
	size_t QueryIndex() const noexcept { return queryIndex_; }
	const VariantArray& Values() const& noexcept { return values_; }
	bool operator==(const SubQueryEntry& other) const noexcept {
		return condition_ == other.condition_ && queryIndex_ == other.queryIndex_ &&
			   values_.RelaxCompare<WithString::Yes, NotComparable::Return, kDefaultNullsHandling>(other.values_) == ComparationResult::Eq;
	}
	std::string Dump(const std::vector<Query>& subQueries) const;

	auto Values() const&& = delete;

private:
	CondType condition_{CondAny};
	// index of Query in Query::subQueries_
	size_t queryIndex_{std::numeric_limits<size_t>::max()};
	VariantArray values_;
};

class [[nodiscard]] SubQueryFieldEntry {
public:
	template <concepts::ConvertibleToString Str>
	SubQueryFieldEntry(Str&& field, CondType cond, size_t qIdx) : field_{std::forward<Str>(field)}, condition_{cond}, queryIndex_{qIdx} {
		checkCondition(cond);
	}
	const std::string& FieldName() const& noexcept { return field_; }
	std::string&& FieldName() && noexcept { return std::move(field_); }
	CondType Condition() const noexcept { return condition_; }
	size_t QueryIndex() const noexcept { return queryIndex_; }
	bool operator==(const SubQueryFieldEntry& other) const noexcept = default;
	std::string Dump(const std::vector<Query>& subQueries) const;

	auto FieldName() const&& = delete;

private:
	void checkCondition(CondType cond) const;

	std::string field_;
	CondType condition_{CondAny};
	// index of Query in Query::subQueries_
	size_t queryIndex_{std::numeric_limits<size_t>::max()};
};

class [[nodiscard]] MultiDistinctQueryEntry {
public:
	MultiDistinctQueryEntry(FieldsSet&& fields) noexcept : fields_(std::move(fields)) {}

	void Verify() const noexcept {}

	bool operator==(const MultiDistinctQueryEntry& other) const noexcept = default;

	std::string Dump() const { return fields_.ToString(DumpWithMask_True); }
	const FieldsSet& FieldNames() const& noexcept { return fields_; }
	auto FieldNames() const&& = delete;

private:
	FieldsSet fields_;
};

class [[nodiscard]] UpdateEntry {
public:
	template <concepts::ConvertibleToString Str>
	UpdateEntry(Str&& c, VariantArray&& v, FieldModifyMode m = FieldModeSet, bool e = false)
		: column_(std::forward<Str>(c)), values_(std::move(v)), mode_(m), isExpression_(e) {
		if (column_.empty()) {
			throw Error{errParams, "Empty update column name"};
		}
	}
	bool operator==(const UpdateEntry& other) const noexcept = default;
	bool operator!=(const UpdateEntry& obj) const noexcept { return !operator==(obj); }
	std::string_view Column() const noexcept { return column_; }
	const VariantArray& Values() const noexcept { return values_; }
	VariantArray& Values() noexcept { return values_; }
	FieldModifyMode Mode() const noexcept { return mode_; }
	void SetMode(FieldModifyMode m) noexcept { mode_ = m; }
	bool IsExpression() const noexcept { return isExpression_; }
	void SetIsExpression(bool e) noexcept { isExpression_ = e; }

private:
	std::string column_;
	VariantArray values_;
	FieldModifyMode mode_{FieldModeSet};
	bool isExpression_{false};
};

class [[nodiscard]] QueryJoinEntry {
public:
	QueryJoinEntry(OpType op, CondType cond, std::string&& leftFld, std::string&& rightFld, bool reverseNs = false)
		: leftField_{std::move(leftFld)}, rightField_{std::move(rightFld)}, op_{op}, condition_{cond}, reverseNamespacesOrder_{reverseNs} {
		if (condition_ == CondKnn) {
			throw Error(errLogic, "Condition KNN cannot be used in ON statement");
		}
	}
	bool operator==(const QueryJoinEntry& other) const noexcept {
		// reverseNamespacesOrder_ is intentionally ignored - it affects serialization order in SQL, but does not make any difference
		// from query standpoint
		return condition_ == other.condition_ && leftField_ == other.leftField_ && rightField_ == other.rightField_;
	}
	bool operator!=(const QueryJoinEntry& other) const noexcept { return !operator==(other); }
	bool IsLeftFieldIndexed() const noexcept { return leftField_.IsFieldIndexed(); }
	bool IsRightFieldIndexed() const noexcept { return rightField_.IsFieldIndexed(); }
	int LeftIdxNo() const noexcept { return leftField_.IndexNo(); }
	int RightIdxNo() const noexcept { return rightField_.IndexNo(); }
	const FieldsSet& LeftFields() const& noexcept { return leftField_.Fields(); }
	const FieldsSet& RightFields() const& noexcept { return rightField_.Fields(); }
	KeyValueType LeftFieldType() const noexcept { return leftField_.FieldType(); }
	KeyValueType RightFieldType() const noexcept { return rightField_.FieldType(); }
	const QueryField::CompositeTypesVecT& LeftCompositeFieldsTypes() const& noexcept { return leftField_.CompositeFieldsTypes(); }
	const QueryField::CompositeTypesVecT& RightCompositeFieldsTypes() const& noexcept { return rightField_.CompositeFieldsTypes(); }
	OpType Operation() const noexcept { return op_; }
	CondType Condition() const noexcept { return condition_; }
	const std::string& LeftFieldName() const& noexcept { return leftField_.FieldName(); }
	const std::string& RightFieldName() const& noexcept { return rightField_.FieldName(); }
	bool ReverseNamespacesOrder() const noexcept { return reverseNamespacesOrder_; }
	const QueryField& LeftFieldData() const& noexcept { return leftField_; }
	QueryField& LeftFieldData() & noexcept { return leftField_; }
	const QueryField& RightFieldData() const& noexcept { return rightField_; }
	QueryField& RightFieldData() & noexcept { return rightField_; }
	bool FieldsHaveBeenSet() const noexcept { return leftField_.FieldsHaveBeenSet() && rightField_.FieldsHaveBeenSet(); }

	template <typename JS>
	std::string DumpCondition(const JS& joinedSelector, bool needOp = false) const;

	auto LeftFields() const&& = delete;
	auto RightFields() const&& = delete;
	auto LeftCompositeFieldsTypes() const&& = delete;
	auto RightCompositeFieldsTypes() const&& = delete;
	auto LeftFieldName() const&& = delete;
	auto RightFieldName() const&& = delete;
	auto LeftFieldData() const&& = delete;
	auto RightFieldData() const&& = delete;

private:
	QueryField leftField_;
	QueryField rightField_;
	const OpType op_{OpOr};
	const CondType condition_{CondAny};
	const bool reverseNamespacesOrder_{false};	///< controls SQL encoding order
												///< false: mainNs.index Condition joinNs.joinIndex
												///< true:  joinNs.joinIndex Invert(Condition) mainNs.index
};

class [[nodiscard]] KnnQueryEntry {
public:
	enum class [[nodiscard]] DataFormatType : int8_t { None = -1, Vector = 0, String = 1 };

	template <concepts::ConvertibleToString Str>
	KnnQueryEntry(Str&& fldName, ConstFloatVectorView v, KnnSearchParams params)
		: KnnQueryEntry{std::forward<Str>(fldName), ConstFloatVector{v.Span()}, params} {}
	template <concepts::ConvertibleToString Str>
	KnnQueryEntry(Str&& fldName, ConstFloatVector v, KnnSearchParams params)
		: fieldName_{std::forward<Str>(fldName)}, format_{DataFormatType::Vector}, value_{std::move(v)}, params_{params} {}
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	KnnQueryEntry(Str1&& fldName, Str2&& data, KnnSearchParams params)
		: fieldName_{std::forward<Str1>(fldName)}, format_{DataFormatType::String}, data_{std::forward<Str2>(data)}, params_{params} {}
	int IndexNo() const noexcept { return idxNo_; }
	ConstFloatVectorView Value() const& noexcept {
		assertrx_throw(format_ == DataFormatType::Vector);
		return ConstFloatVectorView{value_};
	}
	const std::string& Data() const& noexcept {
		assertrx_throw(format_ == DataFormatType::String);
		return data_;
	}
	DataFormatType Format() const noexcept {
		assertrx_throw(format_ != DataFormatType::None);
		return format_;
	}
	KnnSearchParams Params() const noexcept { return params_; }
	bool FieldsHaveBeenSet() const noexcept { return idxNo_ != IndexValueType::NotSet; }
	const std::string& FieldName() const& noexcept { return fieldName_; }
	void SetIndexNo(int idx, std::string_view idxName) noexcept {
		idxNo_ = idx;
		// Explicit equality check to avoid extra allocation on Centos7, when index name is already set
		if (fieldName_ != idxName) {
			fieldName_.assign(idxName);
		}
	}
	std::string Dump() const;
	void ToDsl(JsonBuilder& builder) const;
	bool operator==(const KnnQueryEntry&) const noexcept;
	bool operator!=(const KnnQueryEntry& other) const noexcept { return !operator==(other); }

	auto Value() const&& = delete;
	auto Data() const&& = delete;
	auto FieldName() const&& = delete;

private:
	std::string fieldName_;
	int idxNo_{IndexValueType::NotSet};
	DataFormatType format_{DataFormatType::None};
	ConstFloatVector value_;
	std::string data_;
	KnnSearchParams params_;
};

enum class [[nodiscard]] InjectionDirection : bool { IntoMain, FromMain };
class Index;

using QueryEntriesTree = ExpressionTree<OpType, QueryEntriesBracket, 4, QueryEntry, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse,
										AlwaysTrue, SubQueryEntry, SubQueryFieldEntry, KnnQueryEntry, MultiDistinctQueryEntry>;

template <>
template <>
class [[nodiscard]] QueryEntriesTree::PostProcessor<QueryEntry> {
public:
	static constexpr bool NoOp = false;
	static size_t Process(QueryEntriesTree& tree, size_t pos) {
		assertrx_dbg(tree.Is<QueryEntry>(pos));
		auto& qe = tree.Get<QueryEntry>(pos);
		if (!qe.NeedIsNull()) [[likely]] {
			return 0;
		}
		qe.ResetNeedIsNull();
		const auto inserted =
			tree.Emplace<QueryEntry>(pos + 1, qe.Condition() == CondAllSet ? OpAnd : OpOr, qe.FieldName(), CondEmpty, VariantArray{});
		const OpType op = tree.GetOperation(pos);
		tree.SetOperation(OpAnd, pos);
		tree.EncloseInBracket(pos, pos + 1 + inserted, op);
		return inserted + 1;  // +1 position for the bracket
	}
};

class [[nodiscard]] QueryEntries : public QueryEntriesTree {
protected:
	using Base = QueryEntriesTree;

private:
	explicit QueryEntries(Base&& b) : Base{std::move(b)} {}

public:
	QueryEntries() = default;
	QueryEntries(QueryEntries&&) = default;
	QueryEntries(const QueryEntries&) = default;
	QueryEntries& operator=(QueryEntries&&) = default;

	void ToDsl(const Query& parentQuery, JsonBuilder& builder) const { return toDsl(cbegin(), cend(), parentQuery, builder); }
	void Serialize(WrSerializer& ser, const std::vector<Query>& subQueries) const { serialize(cbegin(), cend(), ser, subQueries); }
	bool CheckIfSatisfyConditions(const ConstPayload& pl) const { return checkIfSatisfyConditions(cbegin(), cend(), pl); }
	static bool CheckIfSatisfyCondition(const VariantArray& lValues, CondType, const VariantArray& rValues);
	template <InjectionDirection>
	size_t InjectConditionsFromOnConditions(size_t position, const h_vector<QueryJoinEntry, 1>& joinEntries,
											const QueryEntries& joinedQueryEntries, size_t joinedQueryNo,
											const std::vector<std::unique_ptr<Index>>* indexesFrom);
	template <typename JS>
	std::string Dump(const std::vector<JS>& joinedSelectors, const std::vector<Query>& subQueries) const {
		WrSerializer ser;
		dump(0, cbegin(), cend(), joinedSelectors, subQueries, ser);
		dumpEqualPositions(0, ser, equalPositions);
		return std::string{ser.Slice()};
	}

	EqualPositions_t equalPositions;

private:
	static void toDsl(const_iterator it, const_iterator to, const Query& parentQuery, JsonBuilder&);
	static void serialize(const_iterator it, const_iterator to, WrSerializer&, const std::vector<Query>& subQueries);
	static void serialize(CondType, const VariantArray& values, WrSerializer&);
	static bool checkIfSatisfyConditions(const_iterator begin, const_iterator end, const ConstPayload&);
	static bool checkIfSatisfyCondition(const QueryEntry&, const ConstPayload&);
	static bool checkIfSatisfyCondition(const BetweenFieldsQueryEntry&, const ConstPayload&);
	size_t injectConditionsFromOnCondition(size_t position, const std::string& fieldName, const std::string& joinedFieldName, CondType,
										   const QueryEntries& joinedQueryEntries, size_t injectedFrom, size_t injectingInto,
										   const std::vector<std::unique_ptr<Index>>* indexesFrom);

protected:
	static void dumpEqualPositions(size_t level, WrSerializer&, const EqualPositions_t&);
	template <typename JS>
	static void dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JS>& joinedSelectors,
					 const std::vector<Query>& subQueries, WrSerializer&);
};

extern template size_t QueryEntries::InjectConditionsFromOnConditions<InjectionDirection::FromMain>(
	size_t, const h_vector<QueryJoinEntry, 1>&, const QueryEntries&, size_t, const std::vector<std::unique_ptr<Index>>*);
extern template size_t QueryEntries::InjectConditionsFromOnConditions<InjectionDirection::IntoMain>(
	size_t, const h_vector<QueryJoinEntry, 1>&, const QueryEntries&, size_t, const std::vector<std::unique_ptr<Index>>*);

struct [[nodiscard]] SortingEntry {
	SortingEntry() noexcept = default;
	template <concepts::ConvertibleToString Str>
	SortingEntry(Str&& e, bool d) noexcept : expression(std::forward<Str>(e)), desc(d) {}
	bool operator==(const SortingEntry&) const noexcept = default;
	bool operator!=(const SortingEntry& se) const noexcept = default;

	std::string expression;
	Desc desc = Desc_False;
	int index = IndexValueType::NotSet;
};

struct [[nodiscard]] SortingEntries : public h_vector<SortingEntry, 1> {};

class [[nodiscard]] AggregateEntry {
public:
	AggregateEntry(AggType type, h_vector<std::string, 1>&& fields, SortingEntries&& sort = {}, unsigned limit = QueryEntry::kDefaultLimit,
				   unsigned offset = QueryEntry::kDefaultOffset);
	bool operator==(const AggregateEntry&) const noexcept = default;
	bool operator!=(const AggregateEntry& ae) const noexcept = default;
	AggType Type() const noexcept { return type_; }
	const h_vector<std::string, 1>& Fields() const noexcept { return fields_; }
	const SortingEntries& Sorting() const noexcept { return sortingEntries_; }
	unsigned Limit() const noexcept { return limit_; }
	unsigned Offset() const noexcept { return offset_; }
	void AddSortingEntry(SortingEntry&& sorting);
	template <concepts::ConvertibleToString Str>
	void Sort(Str&& sortExpr, bool desc) {
		AddSortingEntry({std::forward<Str>(sortExpr), desc});
	}
	void SetLimit(unsigned l);
	void SetOffset(unsigned o);

private:
	AggType type_{AggUnknown};
	h_vector<std::string, 1> fields_;
	SortingEntries sortingEntries_;
	unsigned limit_{QueryEntry::kDefaultLimit};
	unsigned offset_{QueryEntry::kDefaultOffset};
};

}  // namespace reindexer
