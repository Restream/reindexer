#pragma once

#include <string_view>
#include <vector>
#include "estl/h_vector.h"
#include "estl/shared_mutex.h"
#include "tools/errors.h"
#include "tools/lsn.h"
#include "tools/stringstools.h"
#include "wal/walrecord.h"

#include "cluster/idatareplicator.h"
#include "events/listener.h"

namespace reindexer {

class ItemImpl;
struct IndexDef;
class IExternalEventsListener;

#ifdef REINDEX_WITH_V3_FOLLOWERS

/// Object of this class contains filters set. Filters are separated by namespace and concatenated with disjunction
class UpdatesFilters {
public:
	class Filter {
	public:
		// TODO: Any additional condition check should be added here
		bool Check() const { return true; }
		void FromJSON(const gason::JsonNode&) {}
		void GetJSON(JsonBuilder&) const {}

		bool operator==(const Filter&) const { return true; }
	};
	using FiltersList = h_vector<Filter, 4>;
	using MapT = fast_hash_map<std::string, FiltersList, nocase_hash_str, nocase_equal_str, nocase_less_str>;

	/// Merge two filters sets
	/// If one of the filters set is empty, result filters set will also be empty
	/// If one of the filters set contains some conditions for specific namespace,
	/// then result filters set will also contain this conditions
	/// @param rhs - Another filters set
	void Merge(const UpdatesFilters& rhs);
	/// Add new filter for specified namespace. Doesn't merge filters, just concatenates it into disjunction sequence
	/// @param ns - Namespace
	/// @param filter - Filter to add
	void AddFilter(std::string_view ns, Filter filter);
	/// Check if filters set allows this namespace
	/// @param ns - Namespace
	/// @return 'true' if filter's conditions are satisfied
	bool Check(std::string_view ns) const noexcept;
	/// Get current namespaces map
	const MapT& Values() const noexcept { return filters_; }

	Error FromJSON(span<char> json);
	void FromJSON(const gason::JsonNode& root);
	void GetJSON(WrSerializer& ser) const;
	void GetJSON(JsonBuilder& builder) const;

	bool operator==(const UpdatesFilters& rhs) const;

private:
	MapT filters_;
};

class IUpdatesObserverV3 {
public:
	virtual ~IUpdatesObserverV3() = default;
	virtual void OnWALUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord& rec) = 0;
	virtual void OnUpdatesLost(std::string_view nsName) = 0;
	virtual void OnConnectionState(const Error& err) = 0;
};

std::ostream& operator<<(std::ostream& o, const reindexer::UpdatesFilters& sv);
#endif	// REINDEX_WITH_V3_FOLLOWERS

class UpdatesObservers {
public:
	UpdatesObservers(const std::string& dbName, cluster::IDataReplicator& replicator, size_t maxUpdatesQueueSize)
		: replicator_(replicator), eventsListener_(dbName, maxUpdatesQueueSize) {}

#ifdef REINDEX_WITH_V3_FOLLOWERS
	struct ObserverInfo {
		IUpdatesObserverV3* ptr;
		UpdatesFilters filters;
	};

	// V3 API
	void Add(IUpdatesObserverV3* observer, const UpdatesFilters& filter, SubscriptionOpts opts);
	Error Remove(IUpdatesObserverV3* observer);
	std::vector<ObserverInfo> Get() const;
	void OnWALUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord& rec);
	void OnUpdatesLost(std::string_view nsName);

	bool Empty() {
		shared_lock<shared_timed_mutex> lck(mtx_);
		return observersV3_.empty();
	}
	UpdatesFilters GetMergedFilter() const;
#endif	// REINDEX_WITH_V3_FOLLOWERS

	// V4 API
	Error AddOrUpdate(IEventsObserver& observer, EventSubscriberConfig&& cfg) {
		return eventsListener_.AddOrUpdate(observer, std::move(cfg));
	}
	Error Remove(IEventsObserver& observer) { return eventsListener_.Remove(observer); }
	void SendAsyncEventOnly(updates::UpdateRecord&& rec);
	Error SendUpdate(updates::UpdateRecord&& rec, std::function<void()> beforeWaitF, const RdxContext& ctx);
	Error SendUpdates(cluster::UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx);
	Error SendAsyncUpdate(updates::UpdateRecord&& rec, const RdxContext& ctx);
	Error SendAsyncUpdates(cluster::UpdatesContainer&& recs, const RdxContext& ctx);
	void SetEventsServerID(int serverID) noexcept { eventsListener_.SetEventsServerID(serverID); }
	void SetEventsShardID(int shardID) noexcept { eventsListener_.SetEventsShardID(shardID); }

private:
	EventsContainer convertUpdatesContainer(const cluster::UpdatesContainer& data);

#ifdef REINDEX_WITH_V3_FOLLOWERS
	std::vector<ObserverInfo> observersV3_;
	mutable shared_timed_mutex mtx_;
#endif	// REINDEX_WITH_V3_FOLLOWERS
	cluster::IDataReplicator& replicator_;
	EventsListener eventsListener_;
};

}  // namespace reindexer
