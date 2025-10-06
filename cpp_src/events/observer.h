#pragma once

#include "cluster/idatareplicator.h"
#include "events/listener.h"
#include "tools/errors.h"

namespace reindexer {

class ItemImpl;
class IndexDef;
class IExternalEventsListener;

class [[nodiscard]] UpdatesObservers {
public:
	UpdatesObservers(const std::string& dbName, cluster::IDataReplicator& replicator, size_t maxUpdatesQueueSize)
		: replicator_(replicator), eventsListener_(dbName, maxUpdatesQueueSize) {}

	Error AddOrUpdate(IEventsObserver& observer, EventSubscriberConfig&& cfg) {
		return eventsListener_.AddOrUpdate(observer, std::move(cfg));
	}
	Error Remove(IEventsObserver& observer) { return eventsListener_.Remove(observer); }
	void SendAsyncEventOnly(updates::UpdateRecord&& rec);
	Error SendUpdate(updates::UpdateRecord&& rec, std::function<void()> beforeWaitF, const RdxContext& ctx);
	Error SendUpdates(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx);
	Error SendAsyncUpdate(updates::UpdateRecord&& rec, const RdxContext& ctx);
	Error SendAsyncUpdates(UpdatesContainer&& recs, const RdxContext& ctx);
	void SetEventsServerID(int serverID) noexcept { eventsListener_.SetEventsServerID(serverID); }
	void SetEventsShardID(int shardID) noexcept { eventsListener_.SetEventsShardID(shardID); }

private:
	EventsContainer convertUpdatesContainer(const UpdatesContainer& data);

	cluster::IDataReplicator& replicator_;
	EventsListener eventsListener_;
};

}  // namespace reindexer
