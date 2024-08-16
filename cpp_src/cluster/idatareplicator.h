#pragma once

#include "core/rdxcontext.h"
#include "updates/updaterecord.h"

namespace reindexer {
namespace cluster {

using UpdatesContainer = h_vector<updates::UpdateRecord, 2>;

struct IDataReplicator {
	virtual Error Replicate(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) = 0;
	virtual Error ReplicateAsync(UpdatesContainer&& recs, const RdxContext& ctx) = 0;

	virtual ~IDataReplicator() = default;
};

struct IDataSyncer {
	virtual void AwaitInitialSync(const NamespaceName& nsName, const RdxContext& ctx) const = 0;
	virtual void AwaitInitialSync(const RdxContext& ctx) const = 0;
	virtual bool IsInitialSyncDone(const NamespaceName& nsName) const = 0;
	virtual bool IsInitialSyncDone() const = 0;

	virtual ~IDataSyncer() = default;
};

}  // namespace cluster
}  // namespace reindexer
