#pragma once

#include "cluster/stats/relicationstatscollector.h"
#include "updates/updaterecord.h"
#include "updates/updatesqueue.h"

namespace reindexer {

class NamespaceName;

namespace cluster {

class Logger;

template <typename T>
class UpdatesQueuePair {
	using MtxT = read_write_spinlock;

public:
	using HashT = nocase_hash_str;
	using QueueT = updates::UpdatesQueue<T, ReplicationStatsCollector, Logger>;
	using UpdatesContainerT = h_vector<T, 2>;
	constexpr static uint64_t kMaxReplicas = QueueT::kMaxReplicas;

	struct Pair {
		std::shared_ptr<QueueT> sync;
		std::shared_ptr<QueueT> async;
	};

	explicit UpdatesQueuePair(uint64_t maxDataSize);
	Pair GetQueue(const NamespaceName& token) const;
	std::shared_ptr<QueueT> GetSyncQueue() const noexcept;
	std::shared_ptr<QueueT> GetAsyncQueue() const noexcept;
	template <typename ContainerT>
	void ReinitSyncQueue(ReplicationStatsCollector statsCollector, std::optional<ContainerT>&& allowList, const Logger& l);
	template <typename ContainerT>
	void ReinitAsyncQueue(ReplicationStatsCollector statsCollector, std::optional<ContainerT>&& allowList, const Logger& l);
	template <typename ContextT>
	std::pair<Error, bool> Push(UpdatesContainerT&& data, std::function<void()> beforeWait, const ContextT& ctx);
	std::pair<Error, bool> PushNowait(UpdatesContainerT&& data);
	std::pair<Error, bool> PushAsync(UpdatesContainerT&& data);

private:
	UpdatesContainerT copyUpdatesContainer(const UpdatesContainerT& data);

	mutable MtxT mtx_;
	std::shared_ptr<QueueT> syncQueue_;
	std::shared_ptr<QueueT> asyncQueue_;
};

extern template class UpdatesQueuePair<updates::UpdateRecord>;

}  // namespace cluster
}  // namespace reindexer
