#pragma once

#include <variant>
#include "tagsmatcher.h"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] PathFilter {
public:
	class [[nodiscard]] PathStepGuard {
	public:
		PathStepGuard(const PathStepGuard&) = delete;
		PathStepGuard& operator=(const PathStepGuard&) = delete;
		PathStepGuard& operator=(PathStepGuard&&) = delete;

		PathStepGuard(PathStepGuard&& other) noexcept : filter_{other.filter_}, step_{other.step_}, wasMatch_{other.wasMatch_} {
			other.step_ = 0;
		}
		PathStepGuard(PathFilter& filter, concepts::TagNameOrIndex auto... tags) : filter_{filter}, wasMatch_{filter_.match_} {
			(step(tags), ...);
		}
		~PathStepGuard() {
			if (step_) {
				assertrx(filter_.position_ >= step_);
				filter_.position_ -= step_;
				filter_.match_ = wasMatch_;
			}
		}
		PathFilter& GetFilter() & noexcept { return filter_; }
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

		PathFilter& filter_;
		size_t step_{0};
		bool wasMatch_;
	};

	PathFilter(const concepts::OneOf<TagsPath, IndexedTagsPath> auto& path) : path_{&path} {}
	// NOLINTNEXTLINE (bugprone-exception-escape)
	bool Match() const noexcept {
		return match_ && std::visit([this](const auto& path) { return position_ >= path->size(); }, path_);
	}
	// NOLINTNEXTLINE (bugprone-exception-escape)
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

}  // namespace reindexer
