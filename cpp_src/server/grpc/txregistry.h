#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "estl/mutex.h"
#include "tools/clock.h"

namespace reindexer {

class Transaction;

namespace grpc {

class [[nodiscard]] TxRegistry {
public:
	using TimePoint = steady_clock_w::time_point;

	struct [[nodiscard]] TxData {
		std::shared_ptr<Transaction> tx;
		std::string dbName, nsName;
	};

	explicit TxRegistry(std::chrono::seconds idleTimeout) noexcept : idleTimeout_(idleTimeout) {}

	[[nodiscard]] uint64_t Add(TxData data, TimePoint now);
	[[nodiscard]] std::optional<TxData> Acquire(uint64_t id);
	void Release(uint64_t id, TimePoint now);
	[[nodiscard]] bool Remove(uint64_t id);
	[[nodiscard]] std::vector<std::pair<uint64_t, TxData>> ExtractExpired(TimePoint now);

private:
	struct [[nodiscard]] Entry {
		TxData data;
		TimePoint deadline;
		int activeRequests = 0;
	};

	const std::chrono::seconds idleTimeout_;
	mutex mtx_;
	std::unordered_map<uint64_t, Entry> transactions_ RX_GUARDED_BY(mtx_);
	uint64_t nextId_ RX_GUARDED_BY(mtx_) = 0;
};

}  // namespace grpc
}  // namespace reindexer
