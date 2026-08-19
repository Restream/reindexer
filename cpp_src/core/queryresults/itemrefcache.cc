#include "itemrefcache.h"
#include "core/itemimpl.h"

namespace reindexer {

ItemRefCache::ItemRefCache(IdType id, RankT rank, uint16_t nsid, ItemImpl&& i, bool raw)
	: itemImpl(std::move(i)), ref{ItemRefRanked{rank, id, itemImpl.payloadValue_, nsid, raw}} {}

ItemRefCache::ItemRefCache(IdType id, uint16_t nsid, ItemImpl&& i, bool raw)
	: itemImpl(std::move(i)), ref{ItemRef{id, itemImpl.payloadValue_, nsid, raw}} {}

}  // namespace reindexer
