#pragma once

#include "core/idsetcache.h"
#include "core/selectfunc/ctx/ftctx.h"
namespace reindexer {

struct FtIdSetCacheVal {
	FtIdSetCacheVal() : ids(std::make_shared<IdSet>()) {}
	FtIdSetCacheVal(const IdSet::Ptr &i) : ids(i) {}
	FtIdSetCacheVal(const IdSet::Ptr &i, FtCtx::Data::Ptr c) : ids(i), ctx(c) {}

	size_t Size() const { return ids ? sizeof(*ids.get()) + ids->heap_size() : 0; }

	IdSet::Ptr ids;
	FtCtx::Data::Ptr ctx;
};

class FtIdSetCache : public LRUCache<IdSetCacheKey, FtIdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key> {};
}  // namespace reindexer
