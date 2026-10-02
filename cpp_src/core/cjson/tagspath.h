#pragma once

#include <cstdlib>
#include <functional>

#include "core/tag_name_index.h"
#include "estl/h_vector.h"
#include "indexed_path_node.h"
#include "tools/assertrx.h"
#include "tools/customhash.h"

namespace reindexer {

class TagsMatcher;

using TagsPath = h_vector<TagName, 16>;
void Dump(auto& os, const TagsPath&, TagsMatcher* = nullptr);

enum [[nodiscard]] IndexedTagsPathCompareType { IgnoreAllOmittedIndexes, NotIgnoreLeftTrailingIndexes, NotIgnoreTrailingIndexes };

class [[nodiscard]] IndexedTagsPathView : public std::span<const IndexedPathNode> {
	using Base = std::span<const IndexedPathNode>;

public:
	using Base::Base;
	template <size_t Count>
	IndexedTagsPathView(std::span<const IndexedPathNode, Count> other) noexcept : Base{other} {}
	IndexedTagsPathView(std::span<const IndexedPathNode, std::dynamic_extent> other) noexcept : Base{other} {}
};

inline auto ComparePrefix(IndexedTagsPathView lhs, IndexedTagsPathView rhs) noexcept {
	struct [[nodiscard]] PrefixCompRes {
		size_t leftPos;
		size_t rightPos;
		bool res;
	};
	bool result = true;
	size_t lI = 0, rI = 0;
	const size_t lSize = lhs.size();
	const size_t rSize = rhs.size();
	while (result && lI < lSize && rI < rSize) {
		const auto& lNode = lhs[lI];
		const auto& rNode = rhs[rI];
		if (lNode.IsTagIndex()) {
			++lI;
			if (rNode.IsTagIndex()) {
				++rI;
				result = (lNode.GetTagIndex() == rNode.GetTagIndex());
			}
		} else {
			++rI;
			if (rNode.IsTagName()) {
				++lI;
				result = (lNode.GetTagName() == rNode.GetTagName());
			}
		}
	}
	return PrefixCompRes{lI, rI, result};
}

bool Compare(IndexedTagsPathView lhs, const TagsPath& rhs) noexcept;

template <unsigned hvSize>
class [[nodiscard]] IndexedTagsPathImpl : public h_vector<IndexedPathNode, hvSize> {
public:
	using Base = h_vector<IndexedPathNode, hvSize>;
	using Base::Base;
	explicit IndexedTagsPathImpl(const TagsPath& tp) {
		this->reserve(tp.size());
		for (auto t : tp) {
			this->emplace_back(t);
		}
	}
	bool Compare(const TagsPath& other) const noexcept { return reindexer::Compare(*this, other); }
	bool IsNestedOrEqualTo(const TagsPath& other) const noexcept {
		const size_t ourSize = this->size();
		if (ourSize > other.size()) {
			return false;
		}
		for (size_t i = 0; i < ourSize; ++i) {
			if ((*this)[i] != other[i]) {
				return false;
			}
		}
		return true;
	}

	template <unsigned otherHvSize>
	bool ComparePrefix(const IndexedTagsPathImpl<otherHvSize>& other) const noexcept {
		return reindexer::ComparePrefix(*this, other).res;
	}

	void Dump(auto& os, TagsMatcher* = nullptr) const;

	bool operator==(const IndexedTagsPathImpl&) = delete;  // use Compare
};
using IndexedTagsPath = IndexedTagsPathImpl<6>;

template <IndexedTagsPathCompareType compareType>
bool Compare(IndexedTagsPathView lhs, IndexedTagsPathView rhs) noexcept {
	const size_t lSize = lhs.size();
	const size_t rSize = rhs.size();
	size_t lPrefixCompareSize = lSize;
	if constexpr (compareType != IgnoreAllOmittedIndexes) {
		while (lPrefixCompareSize > 0 && lhs[lPrefixCompareSize - 1].IsTagIndex()) {
			--lPrefixCompareSize;
		}
	}
	auto [lI, rI, result] = ComparePrefix(lhs.first(lPrefixCompareSize), rhs);
	if (!result) {
		return false;
	}
	if constexpr (compareType != IgnoreAllOmittedIndexes) {
		while (lI < lSize && rI < rSize) {
			const auto& lNode = lhs[lI];
			assertrx_dbg(lNode.IsTagIndex());
			const auto& rNode = rhs[rI];
			if (rNode.IsTagIndex()) {
				++lI;
				++rI;
				if (lNode.GetTagIndex() != rNode.GetTagIndex()) {
					return false;
				}
			} else {
				return false;
			}
		}
		if (lI != lSize) {
			return false;
		}
	} else {
		for (; lI < lSize; ++lI) {
			if (!lhs[lI].IsTagIndex()) {
				return false;
			}
		}
	}
	if (compareType == NotIgnoreTrailingIndexes) {
		return rI == rSize;
	} else {
		for (; rI < rSize; ++rI) {
			if (!rhs[rI].IsTagIndex()) {
				return false;
			}
		}
		return true;
	}
}

template <typename Path>
class [[nodiscard]] TagsPathScope {
public:
	TagsPathScope(Path& path, TagName tagName) : path_(path), pathNode_(tagName) {
		if (!tagName.IsEmpty()) {
			path_.emplace_back(tagName);
		}
	}
	TagsPathScope(Path& path, TagIndex tagIndex) : path_(path), pathNode_(tagIndex) { path_.emplace_back(tagIndex); }
	TagsPathScope(Path& path, IndexedPathNode tag) : path_(path), pathNode_(tag) {
		if (!pathNode_.IsTagNameEmpty()) {
			path_.emplace_back(pathNode_);
		}
	}
	~TagsPathScope() {
		if (!path_.empty() && !pathNode_.IsTagNameEmpty()) {
			path_.pop_back();
		}
	}
	TagsPathScope(const TagsPathScope&) = delete;
	TagsPathScope& operator=(const TagsPathScope&) = delete;

private:
	Path& path_;
	const IndexedPathNode pathNode_;
};

}  // namespace reindexer

namespace std {

template <>
struct [[nodiscard]] hash<reindexer::TagsPath> {
public:
	size_t operator()(const reindexer::TagsPath& v) const noexcept {
		return reindexer::_Hash_bytes(v.data(), v.size() * sizeof(typename reindexer::TagsPath::value_type));
	}
};

}  // namespace std
