
#include "updatesobserver.h"
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

Error UpdatesObservers::OnModifyItem(const string &nsName, ItemImpl *item, int modifyMode) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (unsigned i = 0; i < observers_.size(); i++) {
		auto observer = observers_[i];
		mtx_.unlock_shared();
		observer->OnModifyItem(nsName, item, modifyMode);
		mtx_.lock_shared();
	}
	return errOK;
}

Error UpdatesObservers::OnNewNamespace(const string &nsName) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (unsigned i = 0; i < observers_.size(); i++) {
		auto observer = observers_[i];
		mtx_.unlock_shared();
		observer->OnNewNamespace(nsName);
		mtx_.lock_shared();
	}
	return errOK;
}

Error UpdatesObservers::OnModifyIndex(const string &nsName, const IndexDef &idx, int modifyMode) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (unsigned i = 0; i < observers_.size(); i++) {
		auto observer = observers_[i];
		mtx_.unlock_shared();
		observer->OnModifyIndex(nsName, idx, modifyMode);
		mtx_.lock_shared();
	}
	return errOK;
}

Error UpdatesObservers::OnDropIndex(const string &nsName, const string &indexName) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (unsigned i = 0; i < observers_.size(); i++) {
		auto observer = observers_[i];
		mtx_.unlock_shared();
		observer->OnDropIndex(nsName, indexName);
		mtx_.lock_shared();
	}
	return errOK;
}

Error UpdatesObservers::OnDropNamespace(const string &nsName) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (unsigned i = 0; i < observers_.size(); i++) {
		auto observer = observers_[i];
		mtx_.unlock_shared();
		observer->OnDropNamespace(nsName);
		mtx_.lock_shared();
	}
	return errOK;
}
Error UpdatesObservers::OnPutMeta(const string &nsName, const string &key, const string &data) {
	shared_lock<shared_timed_mutex> lck(mtx_);
	for (unsigned i = 0; i < observers_.size(); i++) {
		auto observer = observers_[i];
		mtx_.unlock_shared();
		observer->OnPutMeta(nsName, key, data);
		mtx_.lock_shared();
	}
	return errOK;
}

}  // namespace reindexer
