#pragma once

#include <variant>
#include "pathfilter.h"

namespace reindexer {

class [[nodiscard]] FieldsExtractor {
public:
	class [[nodiscard]] FieldParams {
	public:
		int& index;
		int& length;
		int field;
	};

	template <typename Path>
	FieldsExtractor(VariantArray& va, KeyValueType expectedType, const Path& path, FieldParams* params = nullptr)
		: values_(va), expectedType_(expectedType), filterOrStepHolder_(std::in_place_type<PathFilter>, path), params_(params) {}

private:
	FieldsExtractor(VariantArray& va, KeyValueType expectedType, PathFilter::PathStepGuard&& step, FieldParams* params, int arrayIndex)
		: values_(va), expectedType_(expectedType), filterOrStepHolder_(std::move(step)), params_(params), arrayIndex_(arrayIndex) {}

public:
	FieldsExtractor(FieldsExtractor&& other) = delete;
	FieldsExtractor(const FieldsExtractor&) = delete;
	FieldsExtractor& operator=(const FieldsExtractor&) = delete;
	FieldsExtractor& operator=(FieldsExtractor&&) = delete;

	void SetTagsMatcher(const TagsMatcher*) noexcept {}

	FieldsExtractor Object() { return Object(TagName::Empty()); }
	FieldsExtractor Object(concepts::TagNameOrIndex auto tag) {
		auto filterStep = step(tag);
		if (filterRef_.ExactMatch()) [[unlikely]] {
			values_.MarkObject();
		}
		return FieldsExtractor(values_, expectedType_, std::move(filterStep), params_, NotArray);
	}
	FieldsExtractor Array(concepts::TagNameOrIndex auto tag) {
		auto filterStep = step(tag);
		return FieldsExtractor(values_.MarkArray(), expectedType_, std::move(filterStep), params_, 0);
	}
	FieldsExtractor Array(std::string_view) {
		assertrx_throw(false && "not implemented");
		auto filterStep = step(TagName::Empty());
		return FieldsExtractor(values_.MarkArray(), expectedType_, std::move(filterStep), params_, 0);
	}

	template <typename T>
	void Array(concepts::TagNameOrIndex auto tag, std::span<T> data, unsigned offset) {
		auto filterStep = filterRef_.Step(tag);
		const TagIndex tagIndex = filterRef_.GetArrayIndex();
		filterStep.InitArray();
		updateParams(tagIndex, data.size(), offset);
		if (tagIndex.IsAll()) {
			for (const auto& d : data) {
				put(Variant(d));
			}
		} else {
			size_t i{0};
			for (const auto& d : data) {
				if (i++ == tagIndex.AsNumber()) {
					put(Variant(d));
				}
			}
		}
		if (filterRef_.Match()) {
			std::ignore = values_.MarkArray();
		}
	}

	void Array(concepts::TagNameOrIndex auto tag, Serializer& ser, TagType tagType, int count) {
		auto filterStep = filterRef_.Step(tag);
		const TagIndex tagIndex = filterRef_.GetArrayIndex();
		filterStep.InitArray();
		updateParams(tagIndex, count, 0);
		const KeyValueType kvt{tagType};
		if (tagIndex.IsAll()) {
			for (int i = 0; i < count; ++i) {
				put(ser.GetRawVariant(kvt));
			}
		} else {
			for (int i = 0; i < count; ++i) {
				auto value = ser.GetRawVariant(kvt);
				if (TagIndex(i) == tagIndex) {
					put(std::move(value));
				}
			}
		}
		if (filterRef_.Match()) {
			std::ignore = values_.MarkArray();
		}
	}

	void Put(concepts::TagNameOrIndex auto tag, Variant arg, int offset) {
		const auto filterStep = filterRef_.Step(tag);
		if (!filterRef_.Match()) {
			return;
		}
		if (params_) {
			if (params_->index >= 0 && params_->length > 0 && offset == params_->index + params_->length) {
				// Concatenate fields from objects, nested in arrays
				params_->length += 1;
			} else {
				params_->index = offset;
				params_->length = 1;
			}
		}
		return put(std::move(arg));
	}

	template <typename T>
	void Put(concepts::TagNameOrIndex auto tag, const T& arg, int offset) {
		return Put(tag, Variant{arg}, offset);
	}

	void Null(concepts::TagNameOrIndex auto) noexcept {}
	int TargetField() { return params_ ? params_->field : IndexValueType::NotSet; }
	bool IsHavingOffset() const noexcept { return params_ && (params_->length >= 0 || params_->index >= 0); }
	void OnScopeEnd(int offset) noexcept {
		assertrx(params_ && !IsHavingOffset());
		params_->index = offset;
		params_->length = 0;
	}

	template <typename... Args>
	void Object(int, Args...) = delete;
	template <typename... Args>
	void Object(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Array(int, Args...) = delete;
	template <typename... Args>
	void Array(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Put(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Put(int, Args...) = delete;
	template <typename... Args>
	void Null(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Null(int, Args...) = delete;

private:
	void put(Variant arg) {
		if (!filterRef_.Match()) {
			return;
		}
		expectedType_.EvaluateOneOf(
			[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
								KeyValueType::String, KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Uuid,
								KeyValueType::FloatVector> auto) { std::ignore = arg.convert(expectedType_); },
			[](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Composite> auto) noexcept {});
		values_.emplace_back(std::move(arg));
	}

	void updateParams(TagIndex tagIndex, size_t count, int offset) {
		if (params_) {
			if (tagIndex.IsAll()) {
				if (params_->index >= 0 && params_->length > 0) {
					params_->length += count;
				} else {
					params_->index = offset;
					params_->length = count;
				}
			} else if (tagIndex.AsNumber() < count) {
				params_->index = tagIndex.AsNumber() + offset;
				params_->length = 1;
			}
		}
	}

	PathFilter::PathStepGuard step(TagName tag) {
		if (arrayIndex_ == NotArray) {
			return filterRef_.Step(tag);
		} else {
			return filterRef_.Step(TagIndex{arrayIndex_++}, tag);
		}
	}

	PathFilter::PathStepGuard step(TagIndex tag) { return filterRef_.Step(tag); }

	VariantArray& values_;
	KeyValueType expectedType_{KeyValueType::Undefined{}};
	std::variant<PathFilter, PathFilter::PathStepGuard> filterOrStepHolder_{std::in_place_type<PathFilter>, TagsPath{}};
	PathFilter& filterRef_{std::visit(overloaded{[](PathFilter& f) -> PathFilter& { return f; },
												 [](PathFilter::PathStepGuard& s) -> PathFilter& { return s.GetFilter(); }},
									  filterOrStepHolder_)};
	FieldParams* params_;
	enum [[nodiscard]] { NotArray = -1 };
	int arrayIndex_{NotArray};
};

}  // namespace reindexer
