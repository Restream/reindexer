#pragma once

#include <stdlib.h>
#include <memory>
#include <span>
#include "core/enums.h"
#include "core/key_value_type.h"
#include "core/tag_name_index.h"
#include "estl/h_vector.h"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] FieldProperties {
	struct [[nodiscard]] NotIndexedT {};

public:
	static constexpr NotIndexedT kNotIndexed{};
	explicit FieldProperties(NotIndexedT) noexcept : isIndexed_{false} {}
	FieldProperties(int indexNumber, size_t arrayDim, reindexer::IsArray isArray, KeyValueType valueType,
					reindexer::IsSparse isSparse) noexcept
		: indexNumber_(indexNumber), arrayDim_(arrayDim), valueType_{valueType}, isIndexed_{true}, isArray_{isArray}, isSparse_{isSparse} {}

	bool IsIndexed() const noexcept { return isIndexed_; }
	bool IsRegularIndex() const noexcept { return isIndexed_ && !isSparse_; }
	bool IsSparseIndex() const noexcept { return isIndexed_ && isSparse_; }
	reindexer::IsArray IsArray() const noexcept { return isArray_; }
	reindexer::IsSparse IsSparse() const noexcept { return isSparse_; }
	int IndexNumber() const noexcept { return (isIndexed_ && !isSparse_) ? indexNumber_ : -1; }
	int SparseNumber() const noexcept { return indexNumber_; }
	KeyValueType ValueType() const noexcept { return valueType_; }
	uint32_t ArrayDim() const noexcept { return arrayDim_; }

private:
	int indexNumber_{-1};
	size_t arrayDim_{0};
	KeyValueType valueType_{KeyValueType::Undefined{}};
	bool isIndexed_{false};
	reindexer::IsArray isArray_{IsArray_False};
	reindexer::IsSparse isSparse_{IsSparse_False};
};

class [[nodiscard]] TagsPathCache {
public:
	void Set(std::span<const TagName> path, FieldProperties field) {
		assertrx(!path.empty());
		auto cache = this;
		for (;;) {
			const auto tag = path[0].AsNumber();
			if (cache->entries_.size() <= tag) {
				cache->entries_.resize(tag + 1);
			}
			auto& cacheEntry = cache->entries_[tag];
			if (path.size() == 1) {
				cacheEntry.field_ = field;
				return;
			}

			if (!cacheEntry.subCache_) {
				cacheEntry.subCache_ = std::make_shared<TagsPathCache>();
			}
			cache = cacheEntry.subCache_.get();
			path = path.subspan(1);
		}
	}
	FieldProperties Lookup(std::span<const TagName> path) const noexcept {
		assertrx(!path.empty());
		auto cache = this;
		for (;;) {
			const auto tag = path[0].AsNumber();
			if (cache->entries_.size() <= tag) {
				return FieldProperties{FieldProperties::kNotIndexed};
			}
			const CacheEntry& cacheEntry = cache->entries_[tag];
			if (path.size() == 1) {
				return cacheEntry.field_;
			}

			if (!cacheEntry.subCache_) {
				return FieldProperties{FieldProperties::kNotIndexed};
			}
			cache = cacheEntry.subCache_.get();
			path = path.subspan(1);
		}
	}

	void Walk(std::vector<TagName>& path, const std::invocable<const FieldProperties&> auto& visitor) const {
		const auto depth = path.size();
		path.emplace_back(TagName::Empty());
		for (size_t i = 0; i < entries_.size(); ++i) {
			TagName& tag = path[depth];
			tag = TagName(i);
			const CacheEntry& cacheEntry = entries_[i];
			if (cacheEntry.field_.IsIndexed()) {
				visitor(cacheEntry.field_);
			}
			if (cacheEntry.subCache_) {
				cacheEntry.subCache_->Walk(path, visitor);
			}
		}
		path.pop_back();
	}

	void Clear() noexcept { entries_.clear(); }
	bool Empty() const noexcept { return entries_.empty(); }

private:
	struct [[nodiscard]] CacheEntry {
		std::shared_ptr<TagsPathCache> subCache_;
		FieldProperties field_{FieldProperties::kNotIndexed};
	};
	h_vector<CacheEntry> entries_;
};

}  // namespace reindexer
