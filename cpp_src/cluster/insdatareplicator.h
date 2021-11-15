#pragma once

#include "core/rdxcontext.h"
#include "updaterecord.h"

namespace reindexer {
namespace cluster {

using UpdatesContainer = h_vector<cluster::UpdateRecord, 2>;

struct INsDataReplicator {
	virtual Error Replicate(UpdateRecord &&rec, std::function<void()> beforeWaitF, const RdxContext &ctx) = 0;
	virtual Error Replicate(UpdatesContainer &&recs, std::function<void()> beforeWaitF, const RdxContext &ctx) = 0;
	virtual Error ReplicateAsync(UpdateRecord &&rec, const RdxContext &ctx) = 0;
	virtual Error ReplicateAsync(UpdatesContainer &&recs, const RdxContext &ctx) = 0;
	virtual void AwaitInitialSync(std::string_view nsName, const RdxContext &ctx) const = 0;
	virtual void AwaitInitialSync(const RdxContext &ctx) const = 0;
	virtual bool IsInitialSyncDone(std::string_view nsName) const = 0;
	virtual bool IsInitialSyncDone() const = 0;

	virtual ~INsDataReplicator() = default;
};

}  // namespace cluster
}  // namespace reindexer
