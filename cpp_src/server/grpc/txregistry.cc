#include "txregistry.h"

#include "estl/lock.h"
#include "tools/assertrx.h"

#include <algorithm>

namespace reindexer {
namespace grpc {

uint64_t TxRegistry::Add(TxData data, TimePoint now) {
	lock_guard lck(mtx_);
	const uint64_t id = nextId_++;
	transactions_.emplace(id, Entry{.data = std::move(data), .deadline = now + idleTimeout_});
	return id;
}

std::optional<TxRegistry::TxData> TxRegistry::Acquire(uint64_t id) {
	lock_guard lck(mtx_);
	auto it = transactions_.find(id);
	if (it == transactions_.end()) {
		return std::nullopt;
	}
	TxData data = it->second.data;
	++it->second.activeRequests;
	return data;
}

void TxRegistry::Release(uint64_t id, TimePoint now) {
	lock_guard lck(mtx_);
	auto it = transactions_.find(id);
	if (it == transactions_.end()) {
		return;
	}
	assertrx(it->second.activeRequests > 0);
	--it->second.activeRequests;
	it->second.deadline = std::max(it->second.deadline, now + idleTimeout_);
}

bool TxRegistry::Remove(uint64_t id) {
	lock_guard lck(mtx_);
	return transactions_.erase(id) > 0;
}

std::vector<std::pair<uint64_t, TxRegistry::TxData>> TxRegistry::ExtractExpired(TimePoint now) {
	std::vector<std::pair<uint64_t, TxData>> expired;
	lock_guard lck(mtx_);
	for (auto it = transactions_.begin(); it != transactions_.end();) {
		if (it->second.activeRequests == 0 && it->second.deadline <= now) {
			expired.emplace_back(it->first, std::move(it->second.data));
			it = transactions_.erase(it);
		} else {
			++it;
		}
	}
	return expired;
}

}  // namespace grpc
}  // namespace reindexer
