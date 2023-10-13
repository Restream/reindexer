#pragma once

#include "core/idsetcache.h"
#include "core/querycache.h"
#include "core/selectfunc/ctx/ftctx.h"

namespace reindexer {

struct FtIdSetCacheVal {
	FtIdSetCacheVal() = default;
	FtIdSetCacheVal(IdSet::Ptr&& i) noexcept : ids(std::move(i)) {}
	FtIdSetCacheVal(IdSet::Ptr&& i, FtCtx::Data::Ptr&& c) noexcept : ids(std::move(i)), ctx(std::move(c)) {}

	size_t Size() const noexcept { return ids ? (sizeof(*ids.get()) + ids->heap_size()) : 0; }

	IdSet::Ptr ids;
	FtCtx::Data::Ptr ctx;
};

class FtIdSetCache : public LRUCache<IdSetCacheKey, FtIdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key> {};

}  // namespace reindexer
