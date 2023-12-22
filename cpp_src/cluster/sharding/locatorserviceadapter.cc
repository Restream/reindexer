#include "locatorserviceadapter.h"
#include "cluster/sharding/sharding.h"

namespace reindexer::sharding {
std::shared_ptr<client::Reindexer> LocatorServiceAdapter::GetShardConnection(std::string_view ns, int shardId, Error &status) {
	return locator_->GetShardConnection(ns, shardId, status);
}

int LocatorServiceAdapter::ActualShardId() const noexcept { return locator_->ActualShardId(); }

int64_t LocatorServiceAdapter::SourceId() const noexcept { return locator_->SourceId(); }

int LocatorServiceAdapter::GetShardId(std::string_view ns, const Item &item) const { return locator_->GetShardId(ns, item); }

ShardIDsContainer LocatorServiceAdapter::GetShardId(const Query &q) const { return locator_->GetShardId(q); }

}  // namespace reindexer::sharding
