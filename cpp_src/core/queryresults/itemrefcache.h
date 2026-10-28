#pragma once

#include "core/itemimplrawdata.h"
#include "core/queryresults/itemref.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer {

class ItemImpl;

struct [[nodiscard]] ItemRefCache {
	ItemRefCache() = default;
	ItemRefCache(IdType id, RankT, uint16_t nsid, ItemImpl&& i, bool raw);
	ItemRefCache(IdType id, uint16_t nsid, ItemImpl&& i, bool raw);
	void Clear() noexcept {}

	ItemImplRawData itemImpl;
	WrSerializer wser;
	ItemRefVariant ref;
};

}  // namespace reindexer
