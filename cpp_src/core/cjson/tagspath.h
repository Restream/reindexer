#pragma once

#include <cstdlib>
#include <functional>
#include "core/enums.h"
#include "estl/h_vector.h"
#include "tools/customhash.h"

namespace reindexer {

using TagsPath = h_vector<TagName, 16>;

class [[nodiscard]] IndexedPathNode {
	struct [[nodiscard]] AllItemsType {};

public:
	static constexpr AllItemsType AllItems{};
	IndexedPathNode() = default;
	IndexedPathNode(AllItemsType) noexcept : index_{ForAllItems} {}
	IndexedPathNode(TagName _nameTag) noexcept : nameTag_(_nameTag) {}
	IndexedPathNode(TagName _nameTag, int32_t _index) noexcept : nameTag_(_nameTag), index_(_index) {}
	bool operator==(const IndexedPathNode& obj) const noexcept {
		if (nameTag_ != obj.nameTag_) {
			return false;
		}
		if (IsForAllItems() || obj.IsForAllItems()) {
			return true;
		}
		if (index_ != IndexValueType::NotSet && obj.index_ != IndexValueType::NotSet) {
			if (index_ != obj.index_) {
				return false;
			}
		}
		return true;
	}
	bool operator!=(const IndexedPathNode& obj) const noexcept { return !(operator==(obj)); }
	bool operator==(TagName _nameTag) const noexcept { return _nameTag == nameTag_; }
	bool operator!=(TagName _nameTag) const noexcept { return _nameTag != nameTag_; }
	explicit operator TagName() const noexcept { return nameTag_; }

	TagName NameTag() const noexcept { return nameTag_; }
	int Index() const noexcept { return index_; }

	bool IsArrayNode() const noexcept { return (IsForAllItems() || index_ != IndexValueType::NotSet); }
	bool IsWithIndex() const noexcept { return index_ != ForAllItems && index_ != IndexValueType::NotSet; }
	bool IsForAllItems() const noexcept { return index_ == ForAllItems; }

	void MarkAllItems(bool enable) noexcept {
		if (enable) {
			index_ = ForAllItems;
		} else if (index_ == ForAllItems) {
			index_ = IndexValueType::NotSet;
		}
	}

	void SetIndex(int32_t index) noexcept { index_ = index; }
	void SetNameTag(TagName nameTag) noexcept { nameTag_ = nameTag; }

private:
	enum : int32_t { ForAllItems = -2 };
	TagName nameTag_{TagName::Empty()};
	int32_t index_ = IndexValueType::NotSet;
};

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
	template <unsigned hvSizeO>
	bool Compare(const IndexedTagsPathImpl<hvSizeO>& obj) const noexcept {
		const size_t ourSize = this->size();
		if (obj.size() != ourSize) {
			return false;
		}
		if (this->back().IsArrayNode() != obj.back().IsArrayNode()) {
			return false;
		}
		for (size_t i = 0; i < ourSize; ++i) {
			const auto& ourNode = this->operator[](i);
			if (i == ourSize - 1) {
				if (ourNode.IsArrayNode()) {
					if (ourNode.NameTag() != obj[i].NameTag()) {
						return false;
					}
					if (ourNode.IsForAllItems() || obj[i].IsForAllItems()) {
						break;
					}
					return (ourNode.Index() == obj[i].Index());
				} else {
					return (ourNode.NameTag() == obj[i].NameTag());
				}
			} else {
				if (ourNode != obj[i]) {
					return false;
				}
			}
		}
		return true;
	}
	bool Compare(const TagsPath& obj) const noexcept {
		const auto sz = this->size();
		if (obj.size() != sz) {
			return false;
		}
		for (size_t i = 0; i < sz; ++i) {
			if ((*this)[i].NameTag() != obj[i]) {
				return false;
			}
		}
		return true;
	}
	bool IsNestedOrEqualTo(const TagsPath& obj) const noexcept {
		const auto sz = this->size();
		if (sz > obj.size()) {
			return false;
		}
		for (size_t i = 0; i < sz; ++i) {
			if ((*this)[i].NameTag() != obj[i]) {
				return false;
			}
		}
		return true;
	}
};
using IndexedTagsPath = IndexedTagsPathImpl<6>;

template <typename TagsPath>
class [[nodiscard]] TagsPathScope {
public:
	TagsPathScope(TagsPath& tagsPath, TagName tagName) : tagsPath_(tagsPath), tagName_(tagName) {
		if (!tagName_.IsEmpty()) {
			tagsPath_.emplace_back(tagName);
		}
	}
	TagsPathScope(TagsPath& tagsPath, TagName tagName, int32_t index) : tagsPath_(tagsPath), tagName_(tagName) {
		if (!tagName_.IsEmpty()) {
			tagsPath_.emplace_back(tagName, index);
		}
	}
	~TagsPathScope() {
		if (!tagName_.IsEmpty() && !tagsPath_.empty()) {
			tagsPath_.pop_back();
		}
	}
	TagsPathScope(const TagsPathScope&) = delete;
	TagsPathScope& operator=(const TagsPathScope&) = delete;

private:
	TagsPath& tagsPath_;
	const TagName tagName_;
};

}  // namespace reindexer

namespace std {
template <>
struct hash<reindexer::TagsPath> {
public:
	size_t operator()(const reindexer::TagsPath& v) const noexcept {
		return reindexer::_Hash_bytes(v.data(), v.size() * sizeof(typename reindexer::TagsPath::value_type));
	}
};
}  // namespace std
