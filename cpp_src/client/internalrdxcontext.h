#pragma once

#include <chrono>
#include "core/rdxcontext.h"
#include "tools/errors.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

class InternalRdxContext {
public:
	typedef std::function<void(const Error& err)> Completion;
	explicit InternalRdxContext(const IRdxCancelContext* cancelCtx, Completion cmpl = nullptr, milliseconds execTimeout = milliseconds(0),
								lsn_t lsn = lsn_t(), int emmiterServerId = -1, int shardId = -1) noexcept
		: cmpl_(cmpl),
		  execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout),
		  cancelCtx_(cancelCtx),
		  lsn_(lsn),
		  emmiterServerId_(emmiterServerId),
		  shardId_(shardId) {}
	explicit InternalRdxContext(Completion cmpl = nullptr, milliseconds execTimeout = milliseconds(0), lsn_t lsn = lsn_t(),
								int emmiterServerId = -1, int shardId = -1) noexcept
		: cmpl_(cmpl),
		  execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout),
		  cancelCtx_(nullptr),
		  lsn_(lsn),
		  emmiterServerId_(emmiterServerId),
		  shardId_(shardId) {}

	InternalRdxContext WithCancelContext(const IRdxCancelContext* cancelCtx) noexcept {
		return InternalRdxContext(cancelCtx, cmpl_, execTimeout_, lsn_, emmiterServerId_, shardId_);
	}
	InternalRdxContext WithCompletion(Completion cmpl, InternalRdxContext&) noexcept {
		return InternalRdxContext(cmpl, execTimeout_, lsn_, emmiterServerId_, shardId_);
	}
	InternalRdxContext WithCompletion(Completion cmpl) const noexcept {
		return InternalRdxContext(cmpl, execTimeout_, lsn_, emmiterServerId_, shardId_);
	}
	InternalRdxContext WithTimeout(milliseconds execTimeout) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout, lsn_, emmiterServerId_, shardId_);
	}
	InternalRdxContext WithLSN(lsn_t lsn) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout_, lsn, emmiterServerId_, shardId_);
	}
	InternalRdxContext WithEmmiterServerId(int serverId) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout_, lsn_, serverId, shardId_);
	}
	InternalRdxContext WithShardId(int shardId) const noexcept {
		return InternalRdxContext(cmpl_, execTimeout_, lsn_, emmiterServerId_, shardId);
	}
	Completion cmpl() const noexcept { return cmpl_; }
	milliseconds execTimeout() const noexcept { return execTimeout_; }
	const IRdxCancelContext* getCancelCtx() const noexcept { return cancelCtx_; }
	lsn_t lsn() const noexcept { return lsn_; }
	int emmiterServerId() const noexcept { return emmiterServerId_; }
	int shardId() const noexcept { return shardId_; }

private:
	Completion cmpl_;
	milliseconds execTimeout_;
	const IRdxCancelContext* cancelCtx_;
	lsn_t lsn_;
	int emmiterServerId_;
	int shardId_;
};

}  // namespace client
}  // namespace reindexer
