#include "updatesqueuepair.h"
#include "cluster/logger.h"
#include "core/formatters/namespacesname_fmt.h"
#include "core/namespace/namespacename.h"

namespace reindexer::cluster {

template <typename T>
UpdatesQueuePair<T>::UpdatesQueuePair(uint64_t maxDataSize)
	: syncQueue_(std::make_shared<QueueT>(maxDataSize)), asyncQueue_(std::make_shared<QueueT>(maxDataSize)) {}

template <typename T>
typename UpdatesQueuePair<T>::Pair UpdatesQueuePair<T>::GetQueue(const NamespaceName& token) const {
	const size_t hash = token.hash();
	Pair result;
	shared_lock<MtxT> lck(mtx_);
	if (syncQueue_->TokenIsInWhiteList(token, hash)) {
		result.sync = syncQueue_;
	}
	if (asyncQueue_->TokenIsInWhiteList(token, hash)) {
		result.async = asyncQueue_;
	}
	return result;
}

template <typename T>
std::shared_ptr<typename UpdatesQueuePair<T>::QueueT> UpdatesQueuePair<T>::GetSyncQueue() const noexcept {
	shared_lock<MtxT> lck(mtx_);
	return syncQueue_;
}

template <typename T>
std::shared_ptr<typename UpdatesQueuePair<T>::QueueT> UpdatesQueuePair<T>::GetAsyncQueue() const noexcept {
	shared_lock<MtxT> lck(mtx_);
	return asyncQueue_;
}

template <typename T>
std::pair<Error, bool> UpdatesQueuePair<T>::PushNowait(UpdatesContainerT&& data) {
	const auto shardPair = GetQueue(data[0].NsName());
	if (shardPair.sync) {
		if (shardPair.async) {
			shardPair.async->template PushAsync<true>(copyUpdatesContainer(data));
		}
		return shardPair.sync->template PushAsync<false>(std::move(data));
	} else if (shardPair.async) {
		return shardPair.async->template PushAsync<true>(std::move(data));
	}
	return std::make_pair(Error(), false);
}

template <typename T>
std::pair<Error, bool> UpdatesQueuePair<T>::PushAsync(UpdatesContainerT&& data) {
	std::shared_ptr<QueueT> shard;
	{
		std::string_view token(data[0].NsName());
		const HashT h;
		const size_t hash = h(token);
		shared_lock<MtxT> lck(mtx_);
		if (!asyncQueue_->TokenIsInWhiteList(token, hash)) {
			return std::make_pair(Error(), false);
		}
		shard = asyncQueue_;
	}
	return shard->template PushAsync<true>(std::move(data));
}

template <typename T>
typename UpdatesQueuePair<T>::UpdatesContainerT UpdatesQueuePair<T>::copyUpdatesContainer(const UpdatesContainerT& data) {
	UpdatesContainerT copy;
	copy.reserve(data.size());
	for (auto& d : data) {
		// async replication should not see emitter
		copy.emplace_back(d.template Clone<T::ClonePolicy::WithoutEmitter>());
	}
	return copy;
}

template <typename T>
template <typename ContainerT>
void UpdatesQueuePair<T>::ReinitSyncQueue(ReplicationStatsCollector statsCollector, std::optional<ContainerT>&& allowList,
										  const Logger& l) {
	lock_guard lck(mtx_);
	const auto maxDataSize = syncQueue_->MaxDataSize;
	syncQueue_ = std::make_shared<QueueT>(maxDataSize, statsCollector);
	syncQueue_->Init(std::move(allowList), &l);
}

template <typename T>
template <typename ContainerT>
void UpdatesQueuePair<T>::ReinitAsyncQueue(ReplicationStatsCollector statsCollector, std::optional<ContainerT>&& allowList,
										   const Logger& l) {
	lock_guard lck(mtx_);
	const auto maxDataSize = asyncQueue_->MaxDataSize;
	asyncQueue_ = std::make_shared<QueueT>(maxDataSize, statsCollector);
	asyncQueue_->Init(std::move(allowList), &l);
}

template <typename T>
template <typename ContextT>
std::pair<Error, bool> UpdatesQueuePair<T>::Push(UpdatesContainerT&& data, std::function<void()> beforeWait, const ContextT& ctx) {
	const auto shardPair = GetQueue(data[0].NsName());
	if (shardPair.sync) {
		if (shardPair.async) {
			shardPair.async->template PushAsync<true>(copyUpdatesContainer(data));
		}
		return shardPair.sync->PushAndWait(std::move(data), std::move(beforeWait), ctx);
	} else if (shardPair.async) {
		return shardPair.async->template PushAsync<true>(std::move(data));
	}
	return std::make_pair(Error(), false);
}

template class UpdatesQueuePair<updates::UpdateRecord>;
template std::pair<Error, bool> UpdatesQueuePair<updates::UpdateRecord>::Push(
	typename UpdatesQueuePair<updates::UpdateRecord>::UpdatesContainerT&&, std::function<void()>, const RdxContext&);

template void UpdatesQueuePair<updates::UpdateRecord>::ReinitSyncQueue(ReplicationStatsCollector, std::optional<NsNamesHashSetT>&&,
																	   const Logger&);

template void UpdatesQueuePair<updates::UpdateRecord>::ReinitAsyncQueue(ReplicationStatsCollector, std::optional<NsNamesHashSetT>&&,
																		const Logger&);

}  // namespace reindexer::cluster
