#pragma once

#include "core/keyvalue/variant.h"
#include "core/type_consts.h"
#include "pathfilter.h"

namespace reindexer::cjson {

template <typename Strategy>
class [[nodiscard]] FieldsExplorer : public Strategy {
	template <typename... Args>
	FieldsExplorer(PathFilter::PathStepGuard&& step, int arrayIndex, Args&&... args)
		: Strategy(std::forward<Args>(args)...), filterOrStepHolder_(std::move(step)), arrayIndex_(arrayIndex) {}

public:
	template <typename Path, typename... Args>
	FieldsExplorer(const Path& path, Args&&... args)
		: Strategy(std::forward<Args>(args)...), filterOrStepHolder_(std::in_place_type<PathFilter>, path) {}

	FieldsExplorer(FieldsExplorer&&) = delete;
	FieldsExplorer(const FieldsExplorer&) = delete;
	FieldsExplorer& operator=(const FieldsExplorer&) = delete;
	FieldsExplorer& operator=(FieldsExplorer&&) = delete;

	void SetTagsMatcher(const TagsMatcher*) noexcept {}

	FieldsExplorer Object() { return Object(TagName::Empty()); }
	FieldsExplorer Object(concepts::TagNameOrIndex auto tag) {
		auto filterStep = step(tag);
		if (filterRef_.ExactMatch()) [[unlikely]] {
			Strategy::ExactMatchObject();
		}
		return FieldsExplorer(std::move(filterStep), kNotArray, strategy());
	}

	FieldsExplorer Array(concepts::TagNameOrIndex auto tag) {
		auto filterStep = step(tag);
		Strategy::StartArray();
		return FieldsExplorer(std::move(filterStep), 0, strategy());
	}
	FieldsExplorer Array(std::string_view) { throw Error{errLogic, "not implemented"}; }
	template <typename... Args>
	void Array(concepts::TagNameOrIndex auto tag, Args&&... args) {
		auto filterStep = filterRef_.Step(tag);
		const TagIndex tagIndex = filterRef_.GetArrayIndex();
		filterStep.InitArray();
		Strategy::Array(filterRef_, tagIndex, std::forward<Args>(args)...);
	}

	void Put(concepts::TagNameOrIndex auto tag, const Variant& v, int offset) {
		const auto filterStep = filterRef_.Step(tag);
		Strategy::Put(filterRef_, v, offset);
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
	Strategy& strategy() & noexcept { return *this; }

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

	static constexpr int kNotArray = -1;
	int arrayIndex_{kNotArray};
};

}  // namespace reindexer::cjson
