#pragma once

#include "core/rdxcontext.h"
#include "updates/updaterecord.h"

namespace reindexer {
namespace cluster {

struct [[nodiscard]] IDataReplicator {
	virtual Error Replicate(UpdatesContainer&& recs, const std::function<void()>& beforeWaitF, const RdxContext& ctx) = 0;
	virtual Error ReplicateAsync(UpdatesContainer&& recs, const RdxContext& ctx) = 0;

	virtual ~IDataReplicator() = default;
};

struct [[nodiscard]] IDataSyncer {
	virtual void AwaitInitialSync(const NamespaceName& nsName, const RdxContext& ctx) const = 0;
	virtual void AwaitInitialSync(const RdxContext& ctx) const = 0;
	virtual bool IsInitialSyncDone(const NamespaceName& nsName) const = 0;
	virtual bool IsInitialSyncDone() const = 0;
	virtual bool IsClusterNamespace(std::string_view nsName) const = 0;

	virtual ~IDataSyncer() = default;
};

[[noreturn]] inline void throwNotInSecondaryRaftConfig(std::string_view nsName) {
	throw Error(errParams, "Namespace '{}' is not in the RAFT config of the secondary cluster", nsName);
}

}  // namespace cluster
}  // namespace reindexer
