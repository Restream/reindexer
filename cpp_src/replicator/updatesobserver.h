#pragma once

#include <mutex>
#include "core/indexdef.h"
#include "core/item.h"
#include "estl/shared_mutex.h"

namespace reindexer {

class IUpdatesObserver {
public:
	~IUpdatesObserver();

	virtual Error OnModifyItem(const string &nsName, ItemImpl *item, int modifyMode) = 0;
	virtual Error OnNewNamespace(const string &nsName) = 0;
	virtual Error OnModifyIndex(const string &nsName, const IndexDef &idx, int modifyMode) = 0;
	virtual Error OnDropIndex(const string &nsName, const string &indexName) = 0;
	virtual Error OnDropNamespace(const string &nsName) = 0;
	virtual Error OnPutMeta(const string &nsName, const string &key, const string &data) = 0;
};

class UpdatesObservers {
public:
	Error Add(IUpdatesObserver *observer);
	Error Delete(IUpdatesObserver *observer);

	Error OnModifyItem(const string &nsName, ItemImpl *item, int modifyMode);
	Error OnNewNamespace(const string &nsName);
	Error OnModifyIndex(const string &nsName, const IndexDef &idx, int modifyMode);
	Error OnDropIndex(const string &nsName, const string &indexName);
	Error OnDropNamespace(const string &nsName);
	Error OnPutMeta(const string &nsName, const string &key, const string &data);

protected:
	std::vector<IUpdatesObserver *> observers_;
	shared_timed_mutex mtx_;
};

enum {
	//
	kWALOpModifyItem = 1,
	kWALOpNewNamespace = 2,
	kWALOpModifyIndex = 3,
	kWALOpDropIndex = 4,
	kWALOpDropNamespace = 5,
	kWALPutMeta = 6
};

}  // namespace reindexer
