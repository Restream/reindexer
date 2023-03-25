#pragma once

#include <cstdlib>
#include <functional>
#include <string>
#include <string_view>

#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "tools/customhash.h"

namespace reindexer {

using TagsPath = h_vector<int16_t, 16>;

class IndexedPathNode {
	struct AllItemsType {};

public:
	static constexpr AllItemsType AllItems{};
	IndexedPathNode() = default;
	IndexedPathNode(AllItemsType) noexcept : index_{ForAllItems} {}
	IndexedPathNode(int16_t _nameTag) noexcept : nameTag_(_nameTag) {}
	IndexedPathNode(int16_t _nameTag, int32_t _index) noexcept : nameTag_(_nameTag), index_(_index) {}
	bool operator==(const IndexedPathNode &obj) const noexcept {
		if (nameTag_ != obj.nameTag_) return false;
		if (IsForAllItems() || obj.IsForAllItems()) return true;
		if (index_ != IndexValueType::NotSet && obj.index_ != IndexValueType::NotSet) {
			if (index_ != obj.index_) return false;
		}
		return true;
	}
	bool operator!=(const IndexedPathNode &obj) const noexcept { return !(operator==(obj)); }
	bool operator==(int16_t _nameTag) const noexcept { return _nameTag == nameTag_; }
	bool operator!=(int16_t _nameTag) const noexcept { return _nameTag != nameTag_; }
	explicit operator int() const noexcept { return nameTag_; }

	int NameTag() const noexcept { return nameTag_; }
	int Index() const noexcept { return index_; }
	std::string_view Expression() const noexcept {
		if (expression_ && expression_->length() > 0) {
			return std::string_view(expression_->c_str(), expression_->length());
		}
		return std::string_view();
	}

	bool IsArrayNode() const noexcept { return (IsForAllItems() || index_ != IndexValueType::NotSet); }
	bool IsWithIndex() const noexcept { return index_ != ForAllItems && index_ != IndexValueType::NotSet; }
	bool IsWithExpression() const noexcept { return expression_ && !expression_->empty(); }
	bool IsForAllItems() const noexcept { return index_ == ForAllItems; }

	void MarkAllItems(bool enable) noexcept {
		if (enable) {
			index_ = ForAllItems;
		} else if (index_ == ForAllItems) {
			index_ = IndexValueType::NotSet;
		}
	}

	void SetExpression(std::string_view v) {
		if (expression_) {
			expression_->assign(v.data(), v.length());
		} else {
			expression_ = make_key_string(v.data(), v.length());
		}
	}

	void SetIndex(int32_t index) noexcept { index_ = index; }
	void SetNameTag(int16_t nameTag) noexcept { nameTag_ = nameTag; }

private:
	enum : int32_t { ForAllItems = -2 };
	int16_t nameTag_ = 0;
	int32_t index_ = IndexValueType::NotSet;
	key_string expression_;
};

template <unsigned hvSize>
class IndexedTagsPathImpl : public h_vector<IndexedPathNode, hvSize> {
public:
	using Base = h_vector<IndexedPathNode, hvSize>;
	using Base::Base;

	template <unsigned hvSizeO>
	bool Compare(const IndexedTagsPathImpl<hvSizeO> &obj) const noexcept {
		const size_t ourSize = this->size();
		if (obj.size() != ourSize) return false;
		if (this->back().IsArrayNode() != obj.back().IsArrayNode()) return false;
		for (size_t i = 0; i < ourSize; ++i) {
			const auto &ourNode = this->operator[](i);
			if (i == ourSize - 1) {
				if (ourNode.IsArrayNode()) {
					if (ourNode.NameTag() != obj[i].NameTag()) return false;
					if (ourNode.IsForAllItems() || obj[i].IsForAllItems()) break;
					return (ourNode.Index() == obj[i].Index());
				} else {
					return (ourNode.NameTag() == obj[i].NameTag());
				}
			} else {
				if (ourNode != obj[i]) return false;
			}
		}
		return true;
	}
	bool Compare(const TagsPath &obj) const noexcept {
		if (obj.size() != this->size()) return false;
		for (size_t i = 0; i < this->size(); ++i) {
			if (this->operator[](i).NameTag() != obj[i]) return false;
		}
		return true;
	}
};
using IndexedTagsPath = IndexedTagsPathImpl<6>;

using IndexExpressionEvaluator = std::function<VariantArray(std::string_view)>;

template <typename TagsPath>
class TagsPathScope {
public:
	TagsPathScope(TagsPath &tagsPath, int16_t tagName) : tagsPath_(tagsPath), tagName_(tagName) {
		if (tagName_) tagsPath_.emplace_back(tagName);
	}
	TagsPathScope(TagsPath &tagsPath, int16_t tagName, int32_t index) : tagsPath_(tagsPath), tagName_(tagName) {
		if (tagName_) tagsPath_.emplace_back(tagName, index);
	}
	~TagsPathScope() {
		if (tagName_ && !tagsPath_.empty()) tagsPath_.pop_back();
	}
	TagsPathScope(const TagsPathScope &) = delete;
	TagsPathScope &operator=(const TagsPathScope &) = delete;

private:
	TagsPath &tagsPath_;
	const int16_t tagName_;
};

}  // namespace reindexer

namespace std {
template <>
struct hash<reindexer::TagsPath> {
public:
	size_t operator()(const reindexer::TagsPath &v) const noexcept {
		return reindexer::_Hash_bytes(v.data(), v.size() * sizeof(typename reindexer::TagsPath::value_type));
	}
};
}  // namespace std
