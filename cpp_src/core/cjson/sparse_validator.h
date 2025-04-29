#pragma once

#include "cjsontools.h"
#include "core/enums.h"
#include "core/key_value_type.h"
#include "estl/concepts.h"
#include "tagsmatcher.h"

namespace reindexer::item_fields_validator {

struct [[nodiscard]] NoValidation {
	NoValidation Array() const noexcept { return *this; }
	NoValidation Elem() const noexcept { return *this; }
	std::string_view Name() const noexcept { return {}; }
	KeyValueType Type() const noexcept { return KeyValueType::Undefined{}; }
	template <typename T>
	void operator()(const T&) const noexcept {}
};

static constexpr NoValidation kNoValidation;

class [[nodiscard]] SparseArrayValidator;

class [[nodiscard]] SparseValidator {
public:
	SparseValidator(KeyValueType t, IsArray a, size_t arrayDim, int n, const TagsMatcher& tm, InArray inArray, std::string_view parserName)
		: type_{t}, isArray_{a}, arrayDim_{arrayDim}, tagsMatcher_{tm}, sparseNumber_{n}, parserName_{parserName} {
		if (inArray) {
			if rx_unlikely (*!isArray_) {
				throwUnexpectedNestedArrayError(parserName_, Name(), type_);
			}
		}
	}
	SparseArrayValidator Array() const;
	void operator()(concepts::OneOf<int, int64_t, double, float> auto) const {
		validateArrayFieldRestrictions(Name(), isArray_, arrayDim_, 1, parserName_);
		using namespace std::string_view_literals;
		type_.EvaluateOneOf(
			[](OneOf<KeyValueType::Undefined, KeyValueType::Double, KeyValueType::Int, KeyValueType::Bool, KeyValueType::Int64,
					 KeyValueType::String, KeyValueType::Float>) noexcept {},
			[&](OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid, KeyValueType::FloatVector>) {
				throwUnexpected(Name(), type_, "number"sv, parserName_);
			});
	}
	void operator()(bool) const {
		validateArrayFieldRestrictions(Name(), isArray_, arrayDim_, 1, parserName_);
		using namespace std::string_view_literals;
		type_.EvaluateOneOf(
			[](OneOf<KeyValueType::Undefined, KeyValueType::Double, KeyValueType::Int, KeyValueType::Bool, KeyValueType::Int64,
					 KeyValueType::Float>) noexcept {},
			[&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid,
					  KeyValueType::FloatVector>) { throwUnexpected(Name(), type_, "bool"sv, parserName_); });
	}
	void operator()(std::string_view v) const {
		validateArrayFieldRestrictions(Name(), isArray_, arrayDim_, 1, parserName_);
		using namespace std::string_view_literals;
		type_.EvaluateOneOf([](OneOf<KeyValueType::Undefined, KeyValueType::String>) noexcept {}, [v](KeyValueType::Uuid) { Uuid{v}; },
							[&](OneOf<KeyValueType::Int, KeyValueType::Double, KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Tuple,
									  KeyValueType::Composite, KeyValueType::Null, KeyValueType::FloatVector, KeyValueType::Float>) {
								throwUnexpected(Name(), type_, "string"sv, parserName_);
							});
	}
	void operator()(Uuid) const {
		validateArrayFieldRestrictions(Name(), isArray_, arrayDim_, 1, parserName_);
		using namespace std::string_view_literals;
		type_.EvaluateOneOf([](OneOf<KeyValueType::Undefined, KeyValueType::String, KeyValueType::Uuid>) noexcept {},
							[&](OneOf<KeyValueType::Int, KeyValueType::Double, KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Tuple,
									  KeyValueType::Composite, KeyValueType::Null, KeyValueType::Float, KeyValueType::FloatVector>) {
								throwUnexpected(Name(), type_, "uuid"sv, parserName_);
							});
	}
	void operator()(const Variant& v) const {
		v.Type().EvaluateOneOf(
			[&](KeyValueType::Bool) { (*this)(v.As<bool>()); }, [&](KeyValueType::Int) { (*this)(v.As<int>()); },
			[&](KeyValueType::Int64) { (*this)(v.As<int64_t>()); }, [&](KeyValueType::Double) { (*this)(v.As<double>()); },
			[&](KeyValueType::Float) { (*this)(v.As<float>()); },
			[&](OneOf<KeyValueType::String, KeyValueType::Uuid>) { (*this)(v.As<std::string>()); }, [](KeyValueType::Null) noexcept {},
			[&](OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::FloatVector>) {
				throwUnexpected(Name(), type_, v.Type(), parserName_);
			});
	}

	KeyValueType Type() const noexcept { return type_; }
	std::string_view Name() const& { return tagsMatcher_.SparseName(sparseNumber_); }
	auto Name() const&& = delete;

protected:
	const KeyValueType type_;
	const IsArray isArray_;
	const size_t arrayDim_;
	const TagsMatcher& tagsMatcher_;
	const int sparseNumber_;
	const std::string_view parserName_;
};

class [[nodiscard]] SparseArrayValidator : private SparseValidator {
public:
	SparseArrayValidator(SparseValidator&& other) : SparseValidator{std::move(other)} {}
	const SparseValidator& Elem() & noexcept {
		++elemsCount_;
		return *this;
	}
	using SparseValidator::Type;
	using SparseValidator::Name;

	~SparseArrayValidator() noexcept(false) {
		if (std::uncaught_exceptions() == 0) {
			validateArrayFieldRestrictions(Name(), IsArray_True, arrayDim_, elemsCount_, parserName_);
		}
	}

private:
	size_t elemsCount_{0};
};

inline SparseArrayValidator SparseValidator::Array() const {
	if rx_unlikely (!isArray_) {
		throwUnexpectedArrayInIndex(Name(), type_, parserName_);
	}
	return SparseValidator{type_,		  IsArray_False, tagsMatcher_.SparseIndex(sparseNumber_).ArrayDim(), sparseNumber_, tagsMatcher_,
						   InArray_False, parserName_};
}

}  // namespace reindexer::item_fields_validator
