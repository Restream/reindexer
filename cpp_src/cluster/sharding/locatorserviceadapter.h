#pragma once

#include <memory>
#include "cluster/sharding/shardingkeys.h"
#include "tools/errors.h"

namespace reindexer {

namespace client {
class Reindexer;
}

class Query;
class Item;

namespace sharding {

class LocatorService;

class [[nodiscard]] LocatorServiceAdapter {
public:
	LocatorServiceAdapter() = default;
	LocatorServiceAdapter(std::shared_ptr<LocatorService> locator) : locator_(std::move(locator)) {
		if (!locator_) {
			throw Error(errLogic, "Unable to initialize LocatorService's interface with nullptr");
		}
	}
	std::shared_ptr<client::Reindexer> GetShardConnection(std::string_view ns, int shardId, Error& status);
	int ActualShardId() const noexcept;
	int64_t SourceId() const noexcept;
	std::pair<int, Variant> GetShardIdKeyPair(std::string_view ns, const Item& item) const;
	std::pair<ShardIDsContainer, Variant> GetShardIdKeyPair(const Query& q) const;

	inline operator bool() const noexcept { return locator_.operator bool(); }
	inline void reset() noexcept { locator_.reset(); }

private:
	std::shared_ptr<LocatorService> locator_;
};

}  // namespace sharding
}  // namespace reindexer
