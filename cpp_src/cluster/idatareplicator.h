#pragma once

#include "core/rdxcontext.h"
#include "updates/updaterecord.h"

namespace reindexer {
namespace cluster {

struct [[nodiscard]] IDataReplicator {
	virtual Error Replicate(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) = 0;
	virtual Error ReplicateAsync(UpdatesContainer&& recs, const RdxContext& ctx) = 0;

	virtual ~IDataReplicator() = default;
};

struct [[nodiscard]] IDataSyncer {
	virtual void AwaitInitialSync(const NamespaceName& nsName, const RdxContext& ctx) const = 0;
	virtual void AwaitInitialSync(const RdxContext& ctx) const = 0;
	virtual bool IsInitialSyncDone(const NamespaceName& nsName) const = 0;
	virtual bool IsInitialSyncDone() const = 0;

	virtual ~IDataSyncer() = default;
};

}  // namespace cluster
}  // namespace reindexer
