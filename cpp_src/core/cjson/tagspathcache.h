#pragma once

#include <assert.h>
#include <stdlib.h>
#include <functional>
#include <memory>
#include "estl/h_vector.h"
#include "tools/errors.h"

namespace reindexer {

class TagsPathCache {
public:
	void set(const int16_t *tagsPath, size_t len, int field) {
		assert(len);
		auto cache = this;
		for (;;) {
			int tag = *tagsPath++;
			if (int(cache->entries_.size()) <= tag) {
				cache->entries_.resize(tag + 1);
			}
			if (len == 1) {
				cache->entries_[tag].field_ = field;
				return;
			}

			if (!cache->entries_[tag].subCache_) {
				cache->entries_[tag].subCache_ = std::make_shared<TagsPathCache>();
			}
			cache = cache->entries_[tag].subCache_.get();
			len--;
		}
	}
	int lookup(const int16_t *tagsPath, size_t len) const {
		assert(len);
		auto cache = this;
		for (;;) {
			int tag = *tagsPath++;
			if (int(cache->entries_.size()) <= tag) {
				return -1;
			}
			if (len == 1) {
				return cache->entries_[tag].field_;
			}

			if (!cache->entries_[tag].subCache_) {
				return -1;
			}
			cache = cache->entries_[tag].subCache_.get();
			len--;
		}
	}

	void walk(int16_t *path, int depth, std::function<void(int, int)> visitor) const {
		int16_t &i = path[depth];
		for (i = 0; i < int(entries_.size()); i++) {
			if (entries_[i].field_ > 0) visitor(depth + 1, entries_[i].field_);
			if (entries_[i].subCache_) entries_[i].subCache_->walk(path, depth + 1, visitor);
		}
	}

	void clear() { entries_.clear(); }

protected:
	struct CacheEntry {
		std::shared_ptr<TagsPathCache> subCache_;
		int field_ = -1;
	};
	h_vector<CacheEntry> entries_;
};

}  // namespace reindexer
