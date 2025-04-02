#pragma once

#include <assert.h>
#include <stdlib.h>
#include <functional>
#include <memory>
#include <span>
#include "core/enums.h"
#include "estl/h_vector.h"

namespace reindexer {

class [[nodiscard]] TagsPathCache {
public:
	void Set(std::span<const TagName> path, int field) {
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
	[[nodiscard]] int Lookup(std::span<const TagName> path) const noexcept {
		assertrx(!path.empty());
		auto cache = this;
		for (;;) {
			const auto tag = path[0].AsNumber();
			if (cache->entries_.size() <= tag) {
				return -1;
			}
			const auto& cacheEntry = cache->entries_[tag];
			if (path.size() == 1) {
				return cacheEntry.field_;
			}

			if (!cacheEntry.subCache_) {
				return -1;
			}
			cache = cacheEntry.subCache_.get();
			path = path.subspan(1);
		}
	}

	void Walk(std::vector<TagName>& path, const std::function<void(int)>& visitor) const {
		for (size_t i = 0; i < entries_.size(); ++i) {
			path.emplace_back(TagName(i));
			const auto& cacheEntry = entries_[i];
			if (cacheEntry.field_ > 0) {
				visitor(cacheEntry.field_);
			}
			if (cacheEntry.subCache_) {
				cacheEntry.subCache_->Walk(path, visitor);
			}
		}
	}

	void Clear() noexcept { entries_.clear(); }
	[[nodiscard]] bool Empty() const noexcept { return entries_.empty(); }

private:
	struct [[nodiscard]] CacheEntry {
		std::shared_ptr<TagsPathCache> subCache_;
		int field_ = -1;
	};
	h_vector<CacheEntry> entries_;
};

}  // namespace reindexer
