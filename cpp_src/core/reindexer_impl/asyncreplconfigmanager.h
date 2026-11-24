#pragma once

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include "cluster/config.h"
#include "core/rdxcontext.h"
#include "estl/contexted_locks.h"
#include "estl/marked_mutex.h"
#include "estl/spin_lock.h"
#include "estl/thread_annotation_attributes.h"
#include "estl/timed_mutex.h"

namespace gason {
struct JsonNode;
}  // namespace gason

namespace reindexer {

class ReindexerImpl;
class Item;
struct AsyncReplConfigSource;

inline constexpr char kAsyncReplicationConfigType[] = "async_replication";
inline const std::string kAsyncReplicationConfFilename{"async_replication.conf"};

// Serializes the writes of the async replication config and provides its cluster's part with the source ID to the RAFT cluster
class [[nodiscard]] AsyncReplConfigManager {
	using Mutex = MarkedMutex<timed_mutex, MutexMark::AsyncReplConfig>;

public:
	// Serializes the writes of the config. Publishes the source of its cluster's part on unlock
	class [[nodiscard]] RX_SCOPED_CAPABILITY RX_CAPABILITY("mutex") Lock {
	public:
		Lock(AsyncReplConfigManager& owner, const RdxContext& ctx) RX_ACQUIRE(owner.mtx_);
		~Lock() RX_RELEASE();
		// Publishes the source and unlocks, if it is not unlocked yet
		void Unlock() noexcept RX_RELEASE();
		// For the owner, which has published the source itself
		void UnlockPublished() noexcept RX_RELEASE();

	private:
		AsyncReplConfigManager& owner_;
		contexted_unique_lock<Mutex, const RdxContext> lck_;
	};

	explicit AsyncReplConfigManager(ReindexerImpl& owner);

	void PublishInitial() RX_REQUIRES(!mtx_, !sourceMtx_);
	// Applies a stored async config item; the caller's lock must cover its namespace write too
	void ApplyConfigItem(Item& item, const RdxContext& ctx) RX_REQUIRES(mtx_);
	void HandleAction(const gason::JsonNode& action, cluster::RaftInfo::Role role, const RdxContext& ctx) RX_REQUIRES(!mtx_, !sourceMtx_);
	// Replaces the cluster's part of the config, keeping the node's role. Is used by the RAFT cluster
	void ApplyClusterPart(const AsyncReplConfigSource& source, const RdxContext& lockCtx) RX_REQUIRES(!mtx_, !sourceMtx_);
	std::shared_ptr<const AsyncReplConfigSource> GetSource() const RX_REQUIRES(!sourceMtx_);

private:
	// The cluster's part of the config, got with the source ID. The change of 'nodes' revokes it, but keeps the source ID for the
	// generation of the next ones
	struct [[nodiscard]] SavedConfig {
		int64_t SourceIdOf(const cluster::AsyncReplConfigData& applied) const {
			return (config && applied.HasSameClusterNodes(*config)) ? sourceId : int64_t(ShardingSourceId::NotSet);
		}

		int64_t sourceId = ShardingSourceId::NotSet;
		// As it was received, including the fields, which are unknown for this node
		std::string rawConfig;
		std::optional<cluster::AsyncReplConfigData> config;
	};

	// Terminates the process on failure, since the published source has to match the applied config. Only the allocation failures are
	// expected here: it uses its own context, and the calls in it must not fail otherwise
	void publish() noexcept RX_REQUIRES(mtx_, !sourceMtx_);
	void revokeSource(const cluster::AsyncReplConfigData& applied, const RdxContext& ctx) RX_REQUIRES(mtx_);
	void applyClusterPartLocked(const AsyncReplConfigSource& source, const RdxContext& ctx) RX_REQUIRES(mtx_);
	void upsertConfig(const cluster::AsyncReplConfigData& config, const RdxContext& ctx) RX_REQUIRES(mtx_);
	SavedConfig loadSource(const RdxContext& ctx) RX_REQUIRES(mtx_);
	void saveSource(const AsyncReplConfigSource& source, const RdxContext& ctx) RX_REQUIRES(mtx_);

	ReindexerImpl& owner_;
	Mutex mtx_ RX_ACQUIRED_BEFORE(sourceMtx_);
	// Read before acquiring mtx_ to reject recursive config changes
	std::atomic<std::thread::id> writer_;
	mutable spinlock sourceMtx_;
	// Cluster's part of the config with its source ID for the RAFT cluster, which reads it without waiting for the writers
	std::shared_ptr<const AsyncReplConfigSource> source_ RX_GUARDED_BY(sourceMtx_);
};

}  // namespace reindexer
