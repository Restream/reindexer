#pragma once

#include <span>
#include <variant>
#include "tagsmatcher.h"
#include "tools/assertrx.h"

namespace reindexer {

class FieldsFilter;

class [[nodiscard]] FieldsExtractor {
	class [[nodiscard]] Filter {
	public:
		class [[nodiscard]] PathStepGuard {
		public:
			PathStepGuard(const PathStepGuard&) = delete;
			PathStepGuard& operator=(const PathStepGuard&) = delete;
			PathStepGuard& operator=(PathStepGuard&&) = delete;

			PathStepGuard(PathStepGuard&& other) noexcept : filter_{other.filter_}, step_{other.step_}, wasMatch_{other.wasMatch_} {
				other.step_ = 0;
			}
			PathStepGuard(Filter& filter, concepts::TagNameOrIndex auto... tags) : filter_{filter}, wasMatch_{filter_.match_} {
				(step(tags), ...);
			}
			~PathStepGuard() {
				if (step_) {
					assertrx(filter_.position_ >= step_);
					filter_.position_ -= step_;
					filter_.match_ = wasMatch_;
				}
			}
			Filter& GetFilter() & noexcept { return filter_; }
			void InitArray() & { step(TagIndex::All()); }

		private:
			void step(TagIndex tag) {
				std::visit(overloaded{[](const TagsPath*) {},
									  [&](const IndexedTagsPath* path) {
										  if (filter_.match_ && filter_.position_ < path->size()) {
											  const auto& pathNode = (*path)[filter_.position_];
											  if (pathNode.IsTagIndex()) {
												  if (pathNode.GetTagIndex() != tag) {
													  filter_.match_ = false;
												  }
												  ++filter_.position_;
												  ++step_;
											  }
										  } else {
											  ++filter_.position_;
											  ++step_;
										  }
									  }},
						   filter_.path_);
			}

			void step(TagName tag) {
				if (tag.IsEmpty()) {
					return;
				}
				std::visit(overloaded{[&](const TagsPath* path) {
										  if (filter_.position_ < path->size() && (*path)[filter_.position_] != tag) {
											  filter_.match_ = false;
										  }
										  ++filter_.position_;
										  ++step_;
									  },
									  [&](const IndexedTagsPath* pathPtr) {
										  const IndexedTagsPath& path = *pathPtr;
										  const size_t pathSize = path.size();
										  if (filter_.match_ && filter_.position_ < path.size()) {
											  size_t i = filter_.position_;
											  while (i < pathSize && path[i].IsTagIndex()) {
												  ++i;
											  }
											  if (i < pathSize && path[i].GetTagName() != tag) {
												  filter_.match_ = false;
											  }
											  const auto step = i - filter_.position_ + 1;
											  filter_.position_ += step;
											  step_ += step;
										  } else {
											  ++filter_.position_;
											  ++step_;
										  }
									  }},
						   filter_.path_);
			}

			Filter& filter_;
			size_t step_{0};
			bool wasMatch_;
		};

		Filter(const concepts::OneOf<TagsPath, IndexedTagsPath> auto& path) : path_{&path} {}
		bool Match() const noexcept {
			return match_ && std::visit([this](const auto& path) { return position_ >= path->size(); }, path_);
		}
		bool ExactMatch() const noexcept {
			return match_ && std::visit([this](const auto& path) { return position_ == path->size(); }, path_);
		}
		PathStepGuard Step(concepts::TagNameOrIndex auto... tags) & { return PathStepGuard{*this, tags...}; }
		TagIndex GetArrayIndex() const {
			if (!match_) {
				return TagIndex::All();
			}
			return std::visit(overloaded{[](const TagsPath*) { return TagIndex::All(); },
										 [&](const IndexedTagsPath* path) {
											 if (position_ >= path->size() || !(*path)[position_].IsTagIndex()) {
												 return TagIndex::All();
											 }
											 return (*path)[position_].GetTagIndex();
										 }},
							  path_);
		}
		template <typename Os>
		void Dump(Os&) const;

	private:
		std::variant<const TagsPath*, const IndexedTagsPath*> path_;
		size_t position_{0};
		bool match_{true};
	};

public:
	class [[nodiscard]] FieldParams {
	public:
		int& index;
		int& length;
		int field;
	};

	FieldsExtractor() = default;
	template <typename Path>
	FieldsExtractor(VariantArray* va, KeyValueType expectedType, const Path& path, FieldParams* params = nullptr)
		: values_(va), expectedType_(expectedType), filterOrStepHolder_(std::in_place_type<Filter>, path), params_(params) {}

private:
	FieldsExtractor(VariantArray* va, KeyValueType expectedType, Filter::PathStepGuard&& step, FieldParams* params, int arrayIndex)
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
			values_->MarkObject();
		}
		return FieldsExtractor(values_, expectedType_, std::move(filterStep), params_, NotArray);
	}
	FieldsExtractor Array(concepts::TagNameOrIndex auto tag) {
		assertrx_throw(values_);
		auto filterStep = step(tag);
		return FieldsExtractor(&values_->MarkArray(), expectedType_, std::move(filterStep), params_, 0);
	}
	FieldsExtractor Array(std::string_view) {
		assertrx_throw(false && "not implemented");
		assertrx_throw(values_);
		auto filterStep = step(TagName::Empty());
		return FieldsExtractor(&values_->MarkArray(), expectedType_, std::move(filterStep), params_, 0);
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
			assertrx_throw(values_);
			values_->MarkArray();
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
			assertrx_throw(values_);
			values_->MarkArray();
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
								KeyValueType::FloatVector> auto) { arg.convert(expectedType_); },
			[](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Composite> auto) noexcept {});
		assertrx_throw(values_);
		values_->emplace_back(std::move(arg));
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

	Filter::PathStepGuard step(TagName tag) {
		if (arrayIndex_ == NotArray) {
			return filterRef_.Step(tag);
		} else {
			return filterRef_.Step(TagIndex{arrayIndex_++}, tag);
		}
	}

	Filter::PathStepGuard step(TagIndex tag) { return filterRef_.Step(tag); }

	VariantArray* values_ = nullptr;
	KeyValueType expectedType_{KeyValueType::Undefined{}};
	std::variant<Filter, Filter::PathStepGuard> filterOrStepHolder_{std::in_place_type<Filter>, TagsPath{}};
	Filter& filterRef_{
		std::visit(overloaded{[](Filter& f) -> Filter& { return f; }, [](Filter::PathStepGuard& s) -> Filter& { return s.GetFilter(); }},
				   filterOrStepHolder_)};
	FieldParams* params_;
	enum [[nodiscard]] { NotArray = -1 };
	int arrayIndex_{NotArray};
};

}  // namespace reindexer
