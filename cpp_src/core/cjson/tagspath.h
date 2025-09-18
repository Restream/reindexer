#pragma once

#include <cstdlib>
#include <functional>

#include "core/enums.h"
#include "estl/h_vector.h"
#include "tools/customhash.h"

namespace reindexer {

class TagsMatcher;

using TagsPath = h_vector<TagName, 16>;
void Dump(auto& os, const TagsPath&, TagsMatcher* = nullptr);

class [[nodiscard]] IndexedPathNode {
public:
	explicit IndexedPathNode(TagName name) noexcept : name_{name}, type_{Name} {}
	explicit IndexedPathNode(TagIndex index) noexcept : index_{index}, type_{Index} {}
	TagName GetTagName() const noexcept {
		assertrx_dbg(IsTagName());
		return name_;
	}
	TagIndex GetTagIndex() const noexcept {
		assertrx_dbg(IsTagIndex());
		return index_;
	}
	TagIndex& GetTagIndexRef() & noexcept {
		assertrx_dbg(IsTagIndex());
		return index_;
	}
	bool IsTagName() const noexcept { return type_ == Name; }
	bool IsTagIndex() const noexcept { return type_ == Index; }
	bool IsTagIndexNotAll() const noexcept { return IsTagIndex() && !index_.IsAll(); }
	bool Math(TagIndex tag) const noexcept { return type_ == Index && index_ == tag; }
	bool Math(TagName tag) const noexcept { return type_ == Name && name_ == tag; }
	bool operator==(const IndexedPathNode& other) const noexcept {
		if (type_ != other.type_) {
			return false;
		}
		switch (type_) {
			case Index:
				return index_ == other.index_;
			case Name:
				return name_ == other.name_;
			default:
				assertrx_dbg(false);
				return false;
		}
	}
	bool operator==(TagName name) const noexcept { return type_ == Name && name_ == name; }
	bool operator==(TagIndex index) const noexcept { return type_ == Index && index_ == index; }

private:
	TagIndex index_{TagIndex::All()};
	TagName name_{TagName::Empty()};
	enum [[nodiscard]] { Index, Name } type_;
};

enum [[nodiscard]] IndexedTagsPathCompareType { IgnoreAllOmittedIndexes, NotIgnoreLeftTrailingIndexes };

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
	template <IndexedTagsPathCompareType compareType, unsigned hvSizeO>
	bool Compare(const IndexedTagsPathImpl<hvSizeO>& other) const noexcept {
		const size_t ourSize = this->size();
		const size_t otherSize = other.size();
		size_t ourPrefixCompareSize = ourSize;
		if constexpr (compareType == NotIgnoreLeftTrailingIndexes) {
			while (ourPrefixCompareSize > 0 && (*this)[ourPrefixCompareSize - 1].IsTagIndex()) {
				--ourPrefixCompareSize;
			}
		}
		auto [ourI, otherI, result] = comparePrefix(ourPrefixCompareSize, other, otherSize);
		if (!result) {
			return false;
		}
		if constexpr (compareType == NotIgnoreLeftTrailingIndexes) {
			while (ourI < ourSize && otherI < otherSize) {
				const auto& ourNode = (*this)[ourI];
				assertrx_dbg(ourNode.IsTagIndex());
				const auto& otherNode = other[otherI];
				if (otherNode.IsTagIndex()) {
					++ourI;
					++otherI;
					if (ourNode.GetTagIndex() != otherNode.GetTagIndex()) {
						return false;
					}
				} else {
					return false;
				}
			}
			if (ourI != ourSize) {
				return false;
			}
		} else {
			for (; ourI < ourSize; ++ourI) {
				if (!(*this)[ourI].IsTagIndex()) {
					return false;
				}
			}
		}
		for (; otherI < otherSize; ++otherI) {
			if (!other[otherI].IsTagIndex()) {
				return false;
			}
		}
		return true;
	}
	bool Compare(const TagsPath& other) const noexcept {
		const size_t ourSize = this->size();
		const size_t otherSize = other.size();
		if (otherSize > ourSize) {
			return false;
		}
		size_t i = 0;
		for (; i < otherSize; ++i) {
			if ((*this)[i] != other[i]) {
				return false;
			}
		}
		for (; i < ourSize; ++i) {
			if (!(*this)[i].IsTagIndex()) {
				return false;
			}
		}
		return true;
	}
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
		return comparePrefix(this->size(), other, other.size()).res;
	}

	void Dump(auto& os, TagsMatcher* = nullptr) const;

	bool operator==(const IndexedTagsPathImpl&) = delete;  // use Compare

private:
	struct [[nodiscard]] PrefixCompRes {
		size_t ourPos;
		size_t otherPos;
		bool res;
	};
	template <unsigned otherHvSize>
	PrefixCompRes comparePrefix(size_t ourSize, const IndexedTagsPathImpl<otherHvSize>& other, size_t otherSize) const noexcept {
		assertrx_dbg(ourSize <= this->size());
		assertrx_dbg(otherSize <= other.size());
		bool result = true;
		size_t ourI = 0, otherI = 0;
		while (result && ourI < ourSize && otherI < otherSize) {
			const auto& ourNode = (*this)[ourI];
			const auto& otherNode = other[otherI];
			if (ourNode.IsTagIndex()) {
				++ourI;
				if (otherNode.IsTagIndex()) {
					++otherI;
					result = (ourNode.GetTagIndex() == otherNode.GetTagIndex());
				}
			} else {
				++otherI;
				if (otherNode.IsTagName()) {
					++ourI;
					result = (ourNode.GetTagName() == otherNode.GetTagName());
				}
			}
		}
		return {ourI, otherI, result};
	}
};
using IndexedTagsPath = IndexedTagsPathImpl<6>;

template <typename Path>
class [[nodiscard]] TagsPathScope {
public:
	TagsPathScope(Path& path, TagName tagName) : path_(path), pathNode_(tagName) {
		if (!tagName.IsEmpty()) {
			path_.emplace_back(tagName);
		}
	}
	TagsPathScope(Path& path, TagIndex tagIndex) : path_(path), pathNode_(tagIndex) { path_.emplace_back(tagIndex); }
	~TagsPathScope() {
		if (!path_.empty() && (pathNode_.IsTagIndex() || !pathNode_.GetTagName().IsEmpty())) {
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
