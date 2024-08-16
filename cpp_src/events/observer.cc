#include "events/observer.h"
#include "core/cjson/jsonbuilder.h"
#include "core/indexdef.h"
#include "events/listener.h"
#include "tools/logger.h"

using namespace std::string_view_literals;

namespace reindexer {

#ifdef REINDEX_WITH_V3_FOLLOWERS

void UpdatesFilters::Merge(const UpdatesFilters& rhs) {
	if (filters_.empty()) {
		return;
	}
	if (rhs.filters_.empty()) {
		filters_.clear();
		return;
	}
	for (auto& rhsFilter : rhs.filters_) {
		auto foundNs = filters_.find(rhsFilter.first);
		if (foundNs == filters_.end()) {
			filters_.emplace(rhsFilter.first, rhsFilter.second);
		} else {
			auto& nsFilters = foundNs.value();
			for (auto& filter : rhsFilter.second) {
				const auto foundFilter = std::find(nsFilters.cbegin(), nsFilters.cend(), filter);
				if (foundFilter == nsFilters.cend()) {
					nsFilters.emplace_back(filter);
				}
			}
		}
	}
}

void UpdatesFilters::AddFilter(std::string_view ns, UpdatesFilters::Filter filter) {
	auto foundNs = filters_.find(ns);
	if (foundNs == filters_.end()) {
		FiltersList lst;
		lst.emplace_back(std::move(filter));  // NOLINT(performance-move-const-arg)
		filters_.emplace(std::string(ns), std::move(lst));
	} else {
		auto& nsFilters = foundNs.value();
		const auto foundFilter = std::find(nsFilters.cbegin(), nsFilters.cend(), filter);
		if (foundFilter == nsFilters.cend()) {
			nsFilters.emplace_back(std::move(filter));	// NOLINT(performance-move-const-arg)
		}
	}
}

bool UpdatesFilters::Check(std::string_view ns) const noexcept {
	if (filters_.empty()) {
		return true;
	}

	const auto found = filters_.find(ns);
	if (found == filters_.cend()) {
		return false;
	}

	for (auto& filter : found.value()) {
		if (filter.Check()) {
			return true;
		}
	}
	return found.value().empty();
}

Error UpdatesFilters::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "UpdatesFilter: %s", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

void UpdatesFilters::FromJSON(const gason::JsonNode& root) {
	for (const auto& ns : root["namespaces"sv]) {
		auto name = ns["name"sv].As<std::string_view>();
		auto& filtersNode = ns["filters"sv];
		if (filtersNode.empty() || begin(filtersNode) == end(filtersNode)) {
			AddFilter(name, Filter());
		} else {
			for (const auto& f : ns["filters"sv]) {
				Filter filter;
				filter.FromJSON(f);
				AddFilter(name, std::move(filter));	 // NOLINT(performance-move-const-arg)
			}
		}
	}
}

void UpdatesFilters::GetJSON(WrSerializer& ser) const {
	JsonBuilder builder(ser);
	GetJSON(builder);
}

void UpdatesFilters::GetJSON(JsonBuilder& builder) const {
	auto arr = builder.Array("namespaces"sv);
	for (const auto& nsFilters : filters_) {
		auto obj = arr.Object();
		obj.Put("name"sv, nsFilters.first);
		auto arrFilters = obj.Array("filters"sv);
		for (const auto& filter : nsFilters.second) {
			auto filtersObj = arrFilters.Object();
			filter.GetJSON(filtersObj);
		}
	}
}

bool UpdatesFilters::operator==(const UpdatesFilters& rhs) const {
	if (filters_.size() != rhs.filters_.size()) {
		return false;
	}

	for (const auto& nsFilters : filters_) {
		const auto rhsNsFilters = rhs.filters_.find(nsFilters.first);
		if (rhsNsFilters == rhs.filters_.cend()) {
			return false;
		}
		if (rhsNsFilters.value().size() != nsFilters.second.size()) {
			return false;
		}
		for (const auto& filter : nsFilters.second) {
			const auto rhsFilter = std::find(rhsNsFilters.value().cbegin(), rhsNsFilters.value().cend(), filter);
			if (rhsFilter == rhsNsFilters.value().cend()) {
				return false;
			}
		}
	}

	return true;
}

std::ostream& operator<<(std::ostream& o, const reindexer::UpdatesFilters& filters) {
	reindexer::WrSerializer ser;
	filters.GetJSON(ser);
	o << ser.Slice();
	return o;
}

void UpdatesObservers::Add(IUpdatesObserverV3* observer, const UpdatesFilters& filters, SubscriptionOpts opts) {
	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = std::find_if(observersV3_.begin(), observersV3_.end(), [observer](const ObserverInfo& info) { return info.ptr == observer; });
	if (it != observersV3_.end()) {
		if (opts.IsIncrementSubscription()) {
			it->filters.Merge(filters);
		} else {
			it->filters = filters;
		}
	} else {
		observersV3_.emplace_back(ObserverInfo{observer, filters});
	}
}

Error UpdatesObservers::Remove(IUpdatesObserverV3* observer) {
	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = std::find_if(observersV3_.begin(), observersV3_.end(), [observer](const ObserverInfo& info) { return info.ptr == observer; });
	if (it == observersV3_.end()) {
		return Error(errParams, "Observer was not added");
	}
	observersV3_.erase(it);
	return errOK;
}

std::vector<UpdatesObservers::ObserverInfo> UpdatesObservers::Get() const {
	shared_lock<shared_timed_mutex> lck(mtx_);
	return observersV3_;
}

void UpdatesObservers::OnWALUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord& walRec) {
	// Disable updates of system namespaces (it may cause recursive lock)
	if (isSystemNamespaceNameFast(nsName)) {
		return;
	}

	bool skipFilters = walRec.type == WalNamespaceAdd || walRec.type == WalNamespaceDrop || walRec.type == WalNamespaceRename ||
					   walRec.type == WalForceSync || nsName.empty();
	shared_lock<shared_timed_mutex> lck(mtx_);
	if (!skipFilters) {
		for (auto observer : observersV3_) {
			if (observer.filters.Check(nsName)) {
				observer.ptr->OnWALUpdate(LSNs, nsName, walRec);
			}
		}
	} else {
		for (auto observer : observersV3_) {
			observer.ptr->OnWALUpdate(LSNs, nsName, walRec);
		}
	}
}

void UpdatesObservers::OnUpdatesLost(std::string_view nsName) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (auto observer : observersV3_) {
		observer.ptr->OnUpdatesLost(nsName);
	}
}

UpdatesFilters UpdatesObservers::GetMergedFilter() const {
	shared_lock<shared_timed_mutex> lck(mtx_);
	UpdatesFilters filter = observersV3_.size() ? observersV3_.front().filters : UpdatesFilters();
	for (const auto& observer : observersV3_) {
		filter.Merge(observer.filters);
	}
	return filter;
}

#endif	// REINDEX_WITH_V3_FOLLOWERS

void UpdatesObservers::SendAsyncEventOnly(updates::UpdateRecord&& rec) {
	if (eventsListener_.HasListenersFor(rec.NsName())) {
		EventsContainer events;
		events.emplace_back(std::move(rec));
		auto err = eventsListener_.SendEvents(std::move(events));
		if rx_unlikely (!err.ok()) {
			logPrintf(LogError, "Unable to send update to EventsListener: '%s'", err.what());
		}
	}
}

Error UpdatesObservers::SendUpdate(updates::UpdateRecord&& rec, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	cluster::UpdatesContainer recs;
	recs.emplace_back(std::move(rec));
	return SendUpdates(std::move(recs), std::move(beforeWaitF), ctx);
}

Error UpdatesObservers::SendUpdates(cluster::UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	if (recs.empty()) {
		return {};
	}

	if (eventsListener_.HasListenersFor(recs[0].NsName())) {
		auto err = eventsListener_.SendEvents(convertUpdatesContainer(recs));
		if rx_unlikely (!err.ok()) {
			logPrintf(LogError, "Unable to send update to EventsListener: '%s'", err.what());
		}
	}
	return replicator_.Replicate(std::move(recs), std::move(beforeWaitF), ctx);
}

Error UpdatesObservers::SendAsyncUpdate(updates::UpdateRecord&& rec, const RdxContext& ctx) {
	cluster::UpdatesContainer recs(1);
	recs[0] = std::move(rec);
	return replicator_.ReplicateAsync(std::move(recs), ctx);
}

Error UpdatesObservers::SendAsyncUpdates(cluster::UpdatesContainer&& recs, const RdxContext& ctx) {
	if (recs.empty()) {
		return {};
	}

	if (eventsListener_.HasListenersFor(recs[0].NsName())) {
		auto err = eventsListener_.SendEvents(convertUpdatesContainer(recs));
		if rx_unlikely (!err.ok()) {
			logPrintf(LogError, "Unable to send update to EventsListener: '%s'", err.what());
		}
	}
	return replicator_.ReplicateAsync(std::move(recs), ctx);
}

EventsContainer UpdatesObservers::convertUpdatesContainer(const cluster::UpdatesContainer& data) {
	EventsContainer copy;
	copy.reserve(data.size());
	for (auto& d : data) {
		copy.emplace_back(d);
	}
	return copy;
}

}  // namespace reindexer
