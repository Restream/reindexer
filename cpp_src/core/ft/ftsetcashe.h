#pragma once

#include "core/ft/ftctx.h"
#include "core/idsetcache.h"

namespace reindexer {

struct [[nodiscard]] FtIdSetCacheVal {
	FtIdSetCacheVal() = default;
	FtIdSetCacheVal(IdSet::Ptr&& i) noexcept : ids(std::move(i)) {}
	FtIdSetCacheVal(IdSet::Ptr&& i, FtCtxData::Ptr&& c) noexcept : ids(std::move(i)), ctx(std::move(c)) {}
	size_t Size() const noexcept { return ids ? (sizeof(*ids.get()) + ids->heap_size()) : 0; }
	bool IsInitialized() const noexcept { return bool(ids); }

	IdSet::Ptr ids;
	FtCtxData::Ptr ctx;
};

using FtIdSetCache =
	LRUCache<LRUCacheImpl<IdSetCacheKey, FtIdSetCacheVal, IdSetCacheKey::Hash, IdSetCacheKey::Equal>, LRUWithAtomicPtr::No>;

}  // namespace reindexer
