#pragma once

#include "core/formatters/namespacesname_fmt.h"
#include "core/system_ns_names.h"
#include "core/type_consts.h"
#include "estl/shared_mutex.h"
#include "events/subscriber_config.h"
#include "iexternal_listener.h"
#include "net/ev/ev.h"
#include "updates/updatesqueue.h"

// TODO: Remove this #1724
#include "cluster/logger.h"
#include "cluster/stats/relicationstatscollector.h"

namespace reindexer {

class [[nodiscard]] EventsListener : public IExternalEventsListener {
public:
	EventsListener(const std::string& dbName, size_t maxUpdatesQueueSize);
	~EventsListener() override;

	Error SendEvents(EventsContainer&& recs) override final RX_REQUIRES(!mtx_) {
		shared_lock lck(mtx_);
		return updatesQueue_.template PushAsync<true>(std::move(recs)).first;
	}
	bool HasListenersFor(const NamespaceName& ns) const noexcept override final RX_REQUIRES(!mtx_) {
		if (subsCount_.load(std::memory_order_acquire) == 0) {
			return false;
		}

		shared_lock lck(mtx_);
		if (subs_.size() == 0 || !commonFilter_.nss.has_value()) {
			return false;
		}
		if (iequals(ns, kConfigNamespace)) {
			if (!commonFilter_.withConfigNamespace) {
				return false;
			}
		} else if (isSystemNamespaceNameFast(ns)) {
			return false;
		}
		return commonFilter_.nss->empty() || commonFilter_.nss->find(ns) != commonFilter_.nss->end();
	}

	Error AddOrUpdate(IEventsObserver& observer, EventSubscriberConfig&& cfg);
	Error Remove(IEventsObserver& observer);

	void SetEventsServerID(int serverID) noexcept { serverID_.store(serverID, std::memory_order_relaxed); }
	void SetEventsShardID(int shardID) noexcept { shardID_.store(shardID, std::memory_order_relaxed); }

	void Stop();

private:
	using UpdatesQueueT = updates::UpdatesQueue<EventRecord, cluster::ReplicationStatsCollector, cluster::Logger>;
	using NSSetT = fast_hash_set<NamespaceName, NamespaceNameHash, NamespaceNameEqual, NamespaceNameLess>;

	struct [[nodiscard]] DBNamespaces {
		std::optional<NSSetT> nss;
		bool withConfigNamespace = false;
	};
	class [[nodiscard]] ObserverInfo {
	public:
		explicit ObserverInfo(uint32_t _uid) : uid(_uid) {}

		const uint32_t uid;
		uint64_t nextUpdateId = 0;
		uint64_t startUpdateId = 0;
		EventSubscriberConfig cfg;
	};

	using DBMapT = fast_hash_map<const void*, DBNamespaces>;
	using SubscribersMapT = fast_hash_map<IEventsObserver*, ObserverInfo>;

	void stop();
	void rebuildCommonFilter();
	void eventsLoop() noexcept;
	void handleUpdates() RX_REQUIRES(!mtx_);
	void eraseUpdatesOnUnsubscribe(uint32_t uid, uint64_t from, uint64_t to, uint32_t replicas);
	uint32_t buildStreamsMask(const ObserverInfo& observer, const EventRecord& rec) noexcept;

	std::thread eventsThread_;
	std::atomic<bool> terminate_ = {false};
	net::ev::async terminateAsync_;
	net::ev::dynamic_loop loop_;

	// TODO: Add stats collector for this queue #1724
	UpdatesQueueT updatesQueue_;
	std::atomic<int32_t> subsCount_ = {0};
	SubscribersMapT subs_;
	mutable shared_timed_mutex mtx_;
	mutable mutex subUnsubMtx_;
	DBNamespaces commonFilter_;
	const std::string dbName_;
	std::atomic<int> serverID_ = {-1};
	std::atomic<int> shardID_ = {ShardingKeyType::NotSetShard};

	std::vector<uint32_t> emptyUIDs_;
	uint32_t maxUID_ = 0;
};

}  // namespace reindexer
