
#include "updatesobserver.h"
#include "core/indexdef.h"
#include "core/itemimpl.h"
#include "core/keyvalue/p_string.h"
namespace reindexer {

IUpdatesObserver::~IUpdatesObserver() {}

Error UpdatesObservers::Add(IUpdatesObserver *observer) {
	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = std::find(observers_.begin(), observers_.end(), observer);
	if (it != observers_.end()) {
		return Error(errParams, "Observer already added");
	}
	observers_.push_back(observer);
	return errOK;
}

Error UpdatesObservers::Delete(IUpdatesObserver *observer) {
	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = std::find(observers_.begin(), observers_.end(), observer);
	if (it == observers_.end()) {
		return Error(errParams, "Observer was not added");
	}
	observers_.erase(it);
	return errOK;
}

void UpdatesObservers::OnModifyItem(int64_t lsn, string_view nsName, ItemImpl *impl, int modifyMode) {
	WrSerializer ser;
	WALRecord walRec(WalItemModify);
	walRec.itemModify.tmVersion = impl->tagsMatcher().version();
	walRec.itemModify.itemCJson = impl->tagsMatcher().isUpdated() ? impl->GetCJSON(ser, true) : impl->GetCJSON();
	walRec.itemModify.modifyMode = modifyMode;

	OnWALUpdate(lsn, nsName, walRec);
}

void UpdatesObservers::OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &walRec) {
	// Disable updates of system namespaces (it may cause recursive lock)
	if (nsName.size() && nsName[0] == '#') return;

	shared_lock<shared_timed_mutex> lck(mtx_);
	for (auto observer : observers_) {
		observer->OnWALUpdate(lsn, nsName, walRec);
	}
}

void UpdatesObservers::OnConnectionState(const Error &err) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (auto observer : observers_) {
		observer->OnConnectionState(err);
	}
}

}  // namespace reindexer
