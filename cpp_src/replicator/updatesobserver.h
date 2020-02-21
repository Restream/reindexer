#pragma once

#include <mutex>
#include <vector>
#include "estl/shared_mutex.h"
#include "estl/string_view.h"
#include "replicator/walrecord.h"
#include "tools/errors.h"

namespace reindexer {
class ItemImpl;
struct IndexDef;
class IUpdatesObserver {
public:
	virtual ~IUpdatesObserver();
	virtual void OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &rec) = 0;
	virtual void OnConnectionState(const Error &err) = 0;
};

class UpdatesObservers {
public:
	Error Add(IUpdatesObserver *observer);
	Error Delete(IUpdatesObserver *observer);

	void OnModifyItem(int64_t lsn, string_view nsName, ItemImpl *item, int modifyMode, bool inTransaction);

	void OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &rec);

	void OnConnectionState(const Error &err);
	bool empty() {
		shared_lock<shared_timed_mutex> lck(mtx_);
		return observers_.empty();
	}

protected:
	std::vector<IUpdatesObserver *> observers_;
	shared_timed_mutex mtx_;
};

}  // namespace reindexer
