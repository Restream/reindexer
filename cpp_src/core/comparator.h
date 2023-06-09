#pragma once

#include "comparatorimpl.h"
#include "compositearraycomparator.h"

namespace reindexer {

class Comparator : public ComparatorVars {
public:
	Comparator() = delete;
	Comparator(CondType cond, KeyValueType type, const VariantArray &values, bool isArray, bool distinct, PayloadType payloadType,
			   const FieldsSet &fields, void *rawData = nullptr, const CollateOpts &collateOpts = CollateOpts());
	~Comparator() = default;

	bool Compare(const PayloadValue &lhs, int rowId);
	void ExcludeDistinct(const PayloadValue &, int rowId);
	void Bind(const PayloadType &type, int field);
	void BindEqualPosition(int field, const VariantArray &val, CondType cond);
	void BindEqualPosition(const TagsPath &tagsPath, const VariantArray &val, CondType cond);
	void ClearDistinct() {
		cmpInt.ClearDistinct();
		cmpBool.ClearDistinct();
		cmpInt64.ClearDistinct();
		cmpDouble.ClearDistinct();
		cmpString.ClearDistinct();
		cmpGeom.ClearDistinct();
		cmpUuid.ClearDistinct();
	}
	bool HasJsonPaths() const noexcept { return fields_.getTagsPathsLength(); }

private:
	bool compare(const Variant &kr) {
		return kr.Type().EvaluateOneOf(
			[&](KeyValueType::Null) noexcept { return cond_ == CondEmpty; },
			[&](KeyValueType::Int) { return cmpInt.Compare(cond_, static_cast<int>(kr)); },
			[&](KeyValueType::Bool) { return cmpBool.Compare(cond_, static_cast<bool>(kr)); },
			[&](KeyValueType::Int64) { return cmpInt64.Compare(cond_, static_cast<int64_t>(kr)); },
			[&](KeyValueType::Double) { return cmpDouble.Compare(cond_, static_cast<double>(kr)); },
			[&](KeyValueType::String) { return cmpString.Compare(cond_, static_cast<p_string>(kr), collateOpts_); },
			[&](KeyValueType::Composite) { return cmpComposite.Compare(cond_, static_cast<const PayloadValue &>(kr), *this); },
			[&](KeyValueType::Uuid) { return cmpUuid.Compare(cond_, Uuid{kr}); },
			[](OneOf<KeyValueType::Undefined, KeyValueType::Tuple>) noexcept -> bool { abort(); });
	}

	bool compare(const void *ptr) {
		return type_.EvaluateOneOf(
			[&](KeyValueType::Null) noexcept { return cond_ == CondEmpty; },
			[&](KeyValueType::Bool) { return cmpBool.Compare(cond_, *static_cast<const bool *>(ptr)); },
			[&](KeyValueType::Int) { return cmpInt.Compare(cond_, *static_cast<const int *>(ptr)); },
			[&](KeyValueType::Int64) { return cmpInt64.Compare(cond_, *static_cast<const int64_t *>(ptr)); },
			[&](KeyValueType::Double) { return cmpDouble.Compare(cond_, *static_cast<const double *>(ptr)); },
			[&](KeyValueType::String) { return cmpString.Compare(cond_, *static_cast<const p_string *>(ptr), collateOpts_); },
			[&](KeyValueType::Uuid) { return cmpUuid.Compare(cond_, *static_cast<const Uuid *>(ptr)); },
			[&](KeyValueType::Composite) { return cmpComposite.Compare(cond_, *static_cast<const PayloadValue *>(ptr), *this); },
			[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined>) noexcept -> bool {
				assertrx(0);
				abort();
			});
	}

	void excludeDistinct(const Variant &kr) {
		kr.Type().EvaluateOneOf([&](KeyValueType::Int) { cmpInt.ExcludeDistinct(static_cast<int>(kr)); },
								[&](KeyValueType::Bool) { cmpBool.ExcludeDistinct(static_cast<bool>(kr)); },
								[&](KeyValueType::Int64) { cmpInt64.ExcludeDistinct(static_cast<int64_t>(kr)); },
								[&](KeyValueType::Double) { cmpDouble.ExcludeDistinct(static_cast<double>(kr)); },
								[&](KeyValueType::String) { cmpString.ExcludeDistinct(static_cast<p_string>(kr)); },
								[&](KeyValueType::Uuid) { cmpUuid.ExcludeDistinct(Uuid{kr}); },
								[](KeyValueType::Composite) { throw Error(errQueryExec, "Distinct by composite index"); },
								[](OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined>) noexcept {});
	}

	void excludeDistinct(const void *ptr) {
		type_.EvaluateOneOf([&](KeyValueType::Bool) { cmpBool.ExcludeDistinct(*static_cast<const bool *>(ptr)); },
							[&](KeyValueType::Int) { cmpInt.ExcludeDistinct(*static_cast<const int *>(ptr)); },
							[&](KeyValueType::Int64) { cmpInt64.ExcludeDistinct(*static_cast<const int64_t *>(ptr)); },
							[&](KeyValueType::Double) { cmpDouble.ExcludeDistinct(*static_cast<const double *>(ptr)); },
							[&](KeyValueType::String) { cmpString.ExcludeDistinct(*static_cast<const p_string *>(ptr)); },
							[&](KeyValueType::Uuid) { cmpUuid.ExcludeDistinct(*static_cast<const Uuid *>(ptr)); },
							[](KeyValueType::Composite) { throw Error(errQueryExec, "Distinct by composite index"); },
							[](KeyValueType::Null) noexcept {},
							[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined>) noexcept {
								assertrx(0);
								abort();
							});
	}

	void setValues(const VariantArray &values);
	bool isNumericComparison(const VariantArray &values) const;

	void clearAllSetValues() {
		cmpInt.ClearAllSetValues();
		cmpBool.ClearAllSetValues();
		cmpInt64.ClearAllSetValues();
		cmpDouble.ClearAllSetValues();
		cmpString.ClearAllSetValues();
	}

	void clearIndividualAllSetValues() {
		type_.EvaluateOneOf([&](KeyValueType::Bool) noexcept { cmpBool.ClearAllSetValues(); },
							[&](KeyValueType::Int) noexcept { cmpInt.ClearAllSetValues(); },
							[&](KeyValueType::Int64) noexcept { cmpInt64.ClearAllSetValues(); },
							[&](KeyValueType::Double) noexcept { cmpDouble.ClearAllSetValues(); },
							[&](KeyValueType::String) noexcept { cmpString.ClearAllSetValues(); },
							[&](KeyValueType::Composite) noexcept { cmpComposite.ClearAllSetValues(); }, [](KeyValueType::Null) noexcept {},
							[&](KeyValueType::Uuid) noexcept { cmpUuid.ClearAllSetValues(); },
							[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined>) noexcept {
								assertrx(0);
								abort();
							});
	}
	ComparatorImpl<bool> cmpBool;
	ComparatorImpl<int> cmpInt;
	ComparatorImpl<int64_t> cmpInt64;
	ComparatorImpl<double> cmpDouble;
	ComparatorImpl<key_string> cmpString;
	ComparatorImpl<PayloadValue> cmpComposite;
	ComparatorImpl<Point> cmpGeom;
	ComparatorImpl<Uuid> cmpUuid;
	CompositeArrayComparator cmpEqualPosition;
	KeyValueType valuesType_{KeyValueType::Undefined{}};
};

}  // namespace reindexer
