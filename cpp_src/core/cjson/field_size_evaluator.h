#pragma once

#include "pathfilter.h"

namespace reindexer {

class [[nodiscard]] FieldSizeEvaluator {
	FieldSizeEvaluator(PathFilter::PathStepGuard&& step, int arrayIndex, size_t& fieldSize)
		: filterOrStepHolder_(std::move(step)), arrayIndex_(arrayIndex), fieldSize_(fieldSize) {}

public:
	template <typename Path>
	FieldSizeEvaluator(const Path& path, size_t& fieldSize)
		: filterOrStepHolder_(std::in_place_type<PathFilter>, path), fieldSize_(fieldSize) {}

public:
	FieldSizeEvaluator(FieldSizeEvaluator&& other) = delete;
	FieldSizeEvaluator(const FieldSizeEvaluator&) = delete;
	FieldSizeEvaluator& operator=(const FieldSizeEvaluator&) = delete;
	FieldSizeEvaluator& operator=(FieldSizeEvaluator&&) = delete;

	void SetTagsMatcher(const TagsMatcher*) noexcept {}

	FieldSizeEvaluator Object() { return Object(TagName::Empty()); }
	FieldSizeEvaluator Object(concepts::TagNameOrIndex auto tag) {
		auto filterStep = step(tag);
		if (filterRef_.ExactMatch()) [[unlikely]] {
			++fieldSize_;
		}
		return FieldSizeEvaluator(std::move(filterStep), kNotArray, fieldSize_);
	}
	FieldSizeEvaluator Array(concepts::TagNameOrIndex auto tag) {
		auto filterStep = step(tag);
		return FieldSizeEvaluator(std::move(filterStep), 0, fieldSize_);
	}
	FieldSizeEvaluator Array(std::string_view) {
		assertrx_throw(false && "not implemented");
		auto filterStep = step(TagName::Empty());
		return FieldSizeEvaluator(std::move(filterStep), 0, fieldSize_);
	}

	template <typename T>
	void Array(concepts::TagNameOrIndex auto tag, std::span<T> data, unsigned) {
		auto filterStep = filterRef_.Step(tag);
		filterStep.InitArray();
		if (filterRef_.ExactMatch()) {
			fieldSize_ += data.size();
		}
	}

	void Array(concepts::TagNameOrIndex auto tag, Serializer& ser, TagType tagType, int count) {
		auto filterStep = filterRef_.Step(tag);
		filterStep.InitArray();
		const KeyValueType kvt{tagType};
		for (int i = 0; i < count; ++i) {
			std::ignore = ser.GetRawVariant(kvt);
		}
		if (filterRef_.ExactMatch()) {
			fieldSize_ += count;
		}
	}

	void Put(concepts::TagNameOrIndex auto tag, const Variant& v, int) {
		const auto filterStep = filterRef_.Step(tag);
		if (filterRef_.ExactMatch()) {
			if (!v.Type().Is<KeyValueType::Null>()) {
				fieldSize_ += 1;
			}
		}
	}

	template <typename T>
	void Put(concepts::TagNameOrIndex auto tag, const T& arg, int offset) {
		return Put(tag, Variant{arg}, offset);
	}

	void Null(concepts::TagNameOrIndex auto) noexcept {}

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
	PathFilter::PathStepGuard step(TagName tag) {
		if (arrayIndex_ == kNotArray) {
			return filterRef_.Step(tag);
		} else {
			return filterRef_.Step(TagIndex{arrayIndex_++}, tag);
		}
	}

	PathFilter::PathStepGuard step(TagIndex tag) { return filterRef_.Step(tag); }

	std::variant<PathFilter, PathFilter::PathStepGuard> filterOrStepHolder_{std::in_place_type<PathFilter>, TagsPath{}};
	PathFilter& filterRef_{std::visit(overloaded{[](PathFilter& f) -> PathFilter& { return f; },
												 [](PathFilter::PathStepGuard& s) -> PathFilter& { return s.GetFilter(); }},
									  filterOrStepHolder_)};
	enum [[nodiscard]] { kNotArray = -1 };
	int arrayIndex_{kNotArray};
	size_t& fieldSize_;
};

}  // namespace reindexer
