#pragma once

#include "core/ft/ftctx.h"
#include "core/idset/idsetcache.h"

namespace reindexer {

struct [[nodiscard]] FtIdSetCacheVal {
	FtIdSetCacheVal() noexcept = default;
	FtIdSetCacheVal(IdSetPlain::Ptr&& i) noexcept : ids(std::move(i)) {}
	FtIdSetCacheVal(IdSetPlain::Ptr&& i, FtCtxData::Ptr&& c) noexcept : ids(std::move(i)), ctx(std::move(c)) {}
	size_t Size() const noexcept { return ids ? (sizeof(*ids.get()) + ids->HeapSize()) : 0; }
	bool IsInitialized() const noexcept { return bool(ids); }

	IdSetPlain::Ptr ids;
	FtCtxData::Ptr ctx;
};

using FtIdSetCache =
	LRUCache<LRUCacheImpl<IdSetCacheKey, FtIdSetCacheVal, IdSetCacheKey::Hash, IdSetCacheKey::Equal>, LRUWithAtomicPtr::No>;

}  // namespace reindexer
