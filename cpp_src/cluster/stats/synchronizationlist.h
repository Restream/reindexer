#pragma once

#include <map>
#include <vector>
#include "estl/lock.h"
#include "estl/mutex.h"
#include "tools/assertrx.h"

namespace reindexer {
namespace cluster {

class [[nodiscard]] SynchronizationList {
public:
	constexpr static int64_t kEmptyID = -1;
	constexpr static int64_t kUnsynchronizedID = -2;

	void Init(std::vector<int64_t>&& list) {
		lock_guard lck(mtx_);
		lastUpdates_ = std::move(list);
	}
	void Clear() {
		lock_guard lck(mtx_);
		std::fill(lastUpdates_.begin(), lastUpdates_.end(), kUnsynchronizedID);
	}
	void MarkUnsynchonized(uint32_t nodeId) {
		lock_guard lck(mtx_);
		assertrx(nodeId < lastUpdates_.size());
		lastUpdates_[nodeId] = kUnsynchronizedID;
	}
	void MarkSynchronized(uint32_t nodeId, int64_t updateId) {
		lock_guard lck(mtx_);
		assertrx(nodeId < lastUpdates_.size());
		lastUpdates_[nodeId] = updateId;
	}
	std::vector<int64_t> GetSynchronized(uint32_t synchronizedCount) const {
		std::vector<int64_t> res;
		{
			lock_guard lck(mtx_);
			res = lastUpdates_;
		}
		filterNodesBySynchonizedCount(res, synchronizedCount);
		return res;
	}

private:
	static void filterNodesByLastAcceptedId(std::vector<int64_t>& res, int64_t lastAcceptedId) noexcept {
		for (auto& updateId : res) {
			if (updateId < lastAcceptedId) {
				updateId = kUnsynchronizedID;
			}
		}
	}
	static void filterNodesBySynchonizedCount(std::vector<int64_t>& res, uint32_t synchronizedCount) {
		std::map<int64_t, uint32_t> countingMap;
		for (auto updateId : res) {
			if (updateId != kUnsynchronizedID) {
				++countingMap[updateId];
			}
		}
		uint32_t totalCount = 0;
		int64_t lastAcceptedId = kEmptyID;
		for (auto it = countingMap.rbegin(); it != countingMap.rend(); ++it) {
			totalCount += it->second;
			if (totalCount >= synchronizedCount) {
				lastAcceptedId = it->first;
				break;
			}
		}
		filterNodesByLastAcceptedId(res, lastAcceptedId);
	}

	std::vector<int64_t> lastUpdates_;
	mutable mutex mtx_;
};

}  // namespace cluster
}  // namespace reindexer
