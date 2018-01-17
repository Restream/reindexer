#pragma once

#include "core/ft/fulltextctx.h"
#include "core/idsetcache.h"

namespace reindexer {

struct FtIdSetCacheVal {
	FtIdSetCacheVal() : ids(std::make_shared<IdSet>()) {}
	FtIdSetCacheVal(const IdSet::Ptr &i) : ids(i) {}
	FtIdSetCacheVal(const IdSet::Ptr &i, FullTextCtx::Ptr c) : ids(i), ctx(c) {}

	size_t Size() const { return ids->size() * sizeof(IdSet::value_type); }

	IdSet::Ptr ids;
	FullTextCtx::Ptr ctx;
};

class FtIdSetCache : public LRUCache<IdSetCacheKey, FtIdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key> {};
}  // namespace reindexer
