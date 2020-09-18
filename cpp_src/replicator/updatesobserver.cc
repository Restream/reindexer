
#include "updatesobserver.h"
#include "core/cjson/jsonbuilder.h"
#include "core/indexdef.h"
#include "core/itemimpl.h"
#include "core/keyvalue/p_string.h"

namespace reindexer {

void UpdatesFilters::Merge(const UpdatesFilters &rhs) {
	if (filters_.empty()) {
		return;
	}
	if (rhs.filters_.empty()) {
		filters_.clear();
		return;
	}
	for (auto &rhsFilter : rhs.filters_) {
		auto foundNs = filters_.find(rhsFilter.first);
		if (foundNs == filters_.end()) {
			filters_.emplace(rhsFilter.first, rhsFilter.second);
		} else {
			auto &nsFilters = foundNs.value();
			for (auto &filter : rhsFilter.second) {
				const auto foundFilter = std::find(nsFilters.cbegin(), nsFilters.cend(), filter);
				if (foundFilter == nsFilters.cend()) {
					nsFilters.emplace_back(filter);
				}
			}
		}
	}
}

void UpdatesFilters::AddFilter(string_view ns, UpdatesFilters::Filter filter) {
	auto foundNs = filters_.find(ns);
	if (foundNs == filters_.end()) {
		FiltersList lst;
		lst.emplace_back(std::move(filter));
		filters_.emplace(string(ns), std::move(lst));
	} else {
		auto &nsFilters = foundNs.value();
		const auto foundFilter = std::find(nsFilters.cbegin(), nsFilters.cend(), filter);
		if (foundFilter == nsFilters.cend()) {
			nsFilters.emplace_back(std::move(filter));
		}
	}
}

bool UpdatesFilters::Check(string_view ns) const {
	if (filters_.empty()) {
		return true;
	}

	const auto found = filters_.find(ns);
	if (found == filters_.cend()) {
		return false;
	}

	for (auto &filter : found.value()) {
		if (filter.Check()) {
			return true;
		}
	}
	return found.value().empty();
}

Error UpdatesFilters::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "UpdatesFilter: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

void UpdatesFilters::FromJSON(const gason::JsonNode &root) {
	for (const auto &ns : root["namespaces"_sv]) {
		auto name = ns["name"_sv].As<string_view>();
		for (const auto &f : ns["filters"_sv]) {
			Filter filter;
			filter.FromJSON(f);
			AddFilter(name, std::move(filter));
		}
	}
}

void UpdatesFilters::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);
	{
		auto arr = builder.Array("namespaces"_sv);
		for (const auto &nsFilters : filters_) {
			auto obj = arr.Object();
			obj.Put("name"_sv, nsFilters.first);
			auto arrFilters = obj.Array("filters"_sv);
			for (const auto &filter : nsFilters.second) {
				auto filtersObj = arrFilters.Object();
				filter.GetJSON(filtersObj);
			}
		}
	}
}

bool UpdatesFilters::operator==(const UpdatesFilters &rhs) const {
	if (filters_.size() != rhs.filters_.size()) {
		return false;
	}

	for (const auto &nsFilters : filters_) {
		const auto rhsNsFilters = rhs.filters_.find(nsFilters.first);
		if (rhsNsFilters == rhs.filters_.cend()) {
			return false;
		}
		if (rhsNsFilters.value().size() != nsFilters.second.size()) {
			return false;
		}
		for (const auto &filter : nsFilters.second) {
			const auto rhsFilter = std::find(rhsNsFilters.value().cbegin(), rhsNsFilters.value().cend(), filter);
			if (rhsFilter == rhsNsFilters.value().cend()) {
				return false;
			}
		}
	}

	return true;
}

Error UpdatesObservers::Add(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts) {
	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = std::find_if(observers_.begin(), observers_.end(), [observer](const ObserverInfo &info) { return info.ptr == observer; });
	if (it != observers_.end()) {
		if (opts.IsIncrementSubscription()) {
			it->filters.Merge(filters);
		} else {
			it->filters = filters;
		}
	} else {
		observers_.emplace_back(ObserverInfo{observer, filters});
	}
	return errOK;
}

Error UpdatesObservers::Delete(IUpdatesObserver *observer) {
	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = std::find_if(observers_.begin(), observers_.end(), [observer](const ObserverInfo &info) { return info.ptr == observer; });
	if (it == observers_.end()) {
		return Error(errParams, "Observer was not added");
	}
	observers_.erase(it);
	return errOK;
}

std::vector<UpdatesObservers::ObserverInfo> UpdatesObservers::Get() const {
	shared_lock<shared_timed_mutex> lck(mtx_);
	return observers_;
}

void UpdatesObservers::OnModifyItem(LSNPair LSNs, string_view nsName, ItemImpl *impl, int modifyMode, bool inTransaction) {
	WrSerializer ser;
	WALRecord walRec(WalItemModify, impl->tagsMatcher().isUpdated() ? impl->GetCJSON(ser, true) : impl->GetCJSON(),
					 impl->tagsMatcher().version(), modifyMode, inTransaction);

	OnWALUpdate(LSNs, nsName, walRec);
}

void UpdatesObservers::OnWALUpdate(LSNPair LSNs, string_view nsName, const WALRecord &walRec) {
	// Disable updates of system namespaces (it may cause recursive lock)
	if (nsName.size() && nsName[0] == '#') return;

	bool skipFilters = walRec.type == WalNamespaceAdd || walRec.type == WalNamespaceDrop || walRec.type == WalNamespaceRename ||
					   walRec.type == WalForceSync || nsName.empty();
	shared_lock<shared_timed_mutex> lck(mtx_);
	if (!skipFilters) {
		for (auto observer : observers_) {
			if (observer.filters.Check(nsName)) {
				observer.ptr->OnWALUpdate(LSNs, nsName, walRec);
			}
		}
	} else {
		for (auto observer : observers_) {
			observer.ptr->OnWALUpdate(LSNs, nsName, walRec);
		}
	}
}

void UpdatesObservers::OnConnectionState(const Error &err) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (auto &observer : observers_) {
		observer.ptr->OnConnectionState(err);
	}
}

UpdatesFilters UpdatesObservers::GetMergedFilter() const {
	shared_lock<shared_timed_mutex> lck(mtx_);
	UpdatesFilters filter = observers_.size() ? observers_.front().filters : UpdatesFilters();
	for (const auto &observer : observers_) {
		filter.Merge(observer.filters);
	}
	return filter;
}

std::ostream &operator<<(std::ostream &o, const reindexer::UpdatesFilters &filters) {
	reindexer::WrSerializer ser;
	filters.GetJSON(ser);
	o << ser.Slice();
	return o;
}

}  // namespace reindexer
