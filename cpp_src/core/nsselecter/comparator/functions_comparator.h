#pragma once

#include "const.h"
#include "core/function/function.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"
#include "core/type_consts.h"

namespace reindexer {

using FunctionFieldsInfo = h_vector<FieldsSet, 1>;

class [[nodiscard]] FunctionsComparatorImplBase {
public:
	FunctionsComparatorImplBase(FunctionFieldsInfo&& fieldsInfo, CondType cond, const VariantArray& values,
								const functions::FunctionVariant& function, const PayloadType& pt)
		: fieldsInfo_(std::move(fieldsInfo)), cond_(cond), values_(values), function_(function), pt_(pt) {}

	double Cost(double expectedIterations) const noexcept {
		size_t jsonPathComparators = 0;
		for (const auto& fieldInfo : fieldsInfo_) {
			jsonPathComparators += fieldInfo.getTagsPathsLength();
		}
		return jsonPathComparators ? (comparators::kNonIdxFieldComparatorCostMultiplier * expectedIterations + jsonPathComparators + 1.0)
								   : (expectedIterations + 1.0);
	}

	std::string ConditionStr() const;

protected:
	FunctionFieldsInfo fieldsInfo_;
	CondType cond_;
	VariantArray values_;
	functions::FunctionVariant function_;
	PayloadType pt_;
};

class [[nodiscard]] FlatArrayFunctionComparatorImpl : public FunctionsComparatorImplBase {
public:
	FlatArrayFunctionComparatorImpl(FunctionFieldsInfo&& fieldsInfo, CondType cond, const VariantArray& values,
									const functions::FunctionVariant& function, const PayloadType& pt)
		: FunctionsComparatorImplBase(std::move(fieldsInfo), cond, values, function, pt) {
		if (fieldsInfo_.size() != 1) {
			throw Error(errQueryExec, "flat_array_len() expects values only for 1 field, but got {}", fieldsInfo_.size());
		}
		for (const auto& v : values_) {
			if (!v.Type().IsOneOf<KeyValueType::Int, KeyValueType::Int64>()) {
				throw Error(errQueryExec, "flat_array_len() expects integer comparison values");
			}
			concreteValues_.emplace_back(v.As<int>());
		}
	}

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& pv, IdType) const {
		ConstPayload item{pt_, pv};
		const size_t size{item.GetFieldSize(fieldsInfo_[0])};
		return std::visit(overloaded{[this, &size](const functions::FlatArrayLen& f) { return f.Evaluate(cond_, concreteValues_, size); }},
						  function_);
	}

	std::string Name() const& {
		return std::visit(overloaded{[](const functions::FlatArrayLen& f) { return f.ToString(); }}, function_);
	}

private:
	h_vector<int, 1> concreteValues_;
};

using FunctionsComparatorType = std::variant<FlatArrayFunctionComparatorImpl>;

class [[nodiscard]] FunctionsComparator {
public:
	FunctionsComparator(FunctionFieldsInfo&& fieldsInfo, CondType cond, const VariantArray& values,
						const functions::FunctionVariant& function, const PayloadType& pt)
		: name_{"FunctionsComparator"}, impl_{createImpl(std::move(fieldsInfo), cond, values, function, pt)} {}

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& pv, IdType rowId) {
		++totalCalls_;
		const bool matched{std::visit([&pv, &rowId](const auto& impl) { return impl.Compare(pv, rowId); }, impl_)};
		if (matched) {
			++matchedCount_;
		}
		return matched;
	}

	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}

	double Cost(double expectedIterations) const noexcept {
		try {
			return std::visit([&expectedIterations](const auto& impl) { return impl.Cost(expectedIterations); }, impl_);
		} catch (...) {
			assertrx_dbg(false);
			return comparators::kNonIdxFieldComparatorCostMultiplier * expectedIterations + 1.0;
		}
	}

	std::string ConditionStr() const {
		return std::visit([](const auto& impl) { return impl.ConditionStr(); }, impl_);
	}

	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType) const noexcept {}

	std::string Name() const& {
		return std::visit([](const auto& impl) { return impl.Name(); }, impl_);
	}

	std::string Dump() const& { return std::string{Name()} + ' ' + ConditionStr(); }

private:
	FunctionsComparatorType createImpl(FunctionFieldsInfo&& fieldsInfo, CondType cond, const VariantArray& values,
									   const functions::FunctionVariant& function, const PayloadType& pt) {
		switch (std::visit([](const auto& impl) { return impl.Type(); }, function)) {
			case FunctionFlatArrayLen:
				return FlatArrayFunctionComparatorImpl(std::move(fieldsInfo), cond, values, function, pt);
			default:
				abort();
		}
	}

	std::string name_;
	int totalCalls_ = 0;
	int matchedCount_ = 0;
	FunctionsComparatorType impl_;
};

}  // namespace reindexer
