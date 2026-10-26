#pragma once

#include <chrono>
#include "core/rdxcontext.h"
#include "tools/errors.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

class [[nodiscard]] InternalRdxContext {
public:
	typedef std::function<void(const Error& err)> Completion;
	explicit InternalRdxContext(const IRdxCancelContext* cancelCtx, Completion cmpl = nullptr, milliseconds execTimeout = milliseconds(0),
								lsn_t lsn = lsn_t(), int emitterServerId = -1, int shardId = ShardingKeyType::NotSetShard,
								bool parallel = false) noexcept
		: cmpl_(std::move(cmpl)),
		  execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout),
		  cancelCtx_(cancelCtx),
		  lsn_(lsn),
		  emitterServerId_(emitterServerId),
		  shardId_(shardId),
		  shardingParallelExecution_(parallel) {}
	explicit InternalRdxContext(Completion cmpl = nullptr, milliseconds execTimeout = milliseconds(0), lsn_t lsn = lsn_t(),
								int emitterServerId = -1, int shardId = ShardingKeyType::NotSetShard, bool parallel = false) noexcept
		: cmpl_(std::move(cmpl)),
		  execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout),
		  cancelCtx_(nullptr),
		  lsn_(lsn),
		  emitterServerId_(emitterServerId),
		  shardId_(shardId),
		  shardingParallelExecution_(parallel) {}

	explicit InternalRdxContext(lsn_t lsn, Completion cmpl = nullptr, int shardId = ShardingKeyType::NotSetShard, bool parallel = false,
								int emitterServerId = -1, milliseconds execTimeout = milliseconds(0)) noexcept
		: InternalRdxContext(std::move(cmpl), std::move(execTimeout), std::move(lsn), emitterServerId, shardId, parallel) {}

	InternalRdxContext WithCancelContext(const IRdxCancelContext* cancelCtx) const noexcept {
		return InternalRdxContext(cancelCtx, cmpl_, execTimeout_, lsn_, emitterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithCompletion(Completion cmpl, InternalRdxContext&) const noexcept {
		return InternalRdxContext(std::move(cmpl), execTimeout_, lsn_, emitterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithCompletion(Completion cmpl) const noexcept {
		return InternalRdxContext(std::move(cmpl), execTimeout_, lsn_, emitterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithTimeout(milliseconds execTimeout) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout, lsn_, emitterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithLSN(lsn_t lsn) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout_, lsn, emitterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithEmitterServerId(int serverId) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout_, lsn_, serverId, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithShardId(int shardId, bool parallel) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout_, lsn_, emitterServerId_, shardId, parallel);
	}
	InternalRdxContext WithShardingParallelExecution(bool parallel) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout_, lsn_, emitterServerId_, shardId_, parallel);
	}
	const Completion& cmpl() const noexcept { return cmpl_; }
	milliseconds execTimeout() const noexcept { return execTimeout_; }
	const IRdxCancelContext* getCancelCtx() const noexcept { return cancelCtx_; }
	lsn_t lsn() const noexcept { return lsn_; }
	int emitterServerId() const noexcept { return emitterServerId_; }
	int shardId() const noexcept { return shardId_; }
	bool IsShardingParallelExecution() const noexcept { return shardingParallelExecution_; }

private:
	Completion cmpl_;
	milliseconds execTimeout_;
	const IRdxCancelContext* cancelCtx_;
	lsn_t lsn_;
	int emitterServerId_;
	int shardId_;
	bool shardingParallelExecution_;
};

}  // namespace client
}  // namespace reindexer
