#pragma once

#include <functional>
#include "activity_context.h"
#include "tools/errors.h"
#include "tools/lsn.h"

namespace reindexer {

using std::chrono::milliseconds;

enum class CancelType : uint8_t { None = 0, Explicit, Timeout };

struct IRdxCancelContext {
	virtual CancelType GetCancelType() const noexcept = 0;
	virtual bool IsCancelable() const noexcept = 0;

	virtual ~IRdxCancelContext() = default;
};

template <typename Context>
void ThrowOnCancel(const Context& ctx, std::string_view errMsg = std::string_view()) {
	if (!ctx.isCancelable()) return;

	using namespace std::string_view_literals;
	auto cancel = ctx.checkCancel();
	switch (cancel) {
		case CancelType::Explicit:
			throw Error(errCanceled, errMsg.empty() ? "Request was canceled by context"sv : errMsg);
		case CancelType::Timeout:
			throw Error(errTimeout, errMsg.empty() ? "Request was canceled on timeout"sv : errMsg);
		case CancelType::None:
			return;
		default:
			assert(false);
			throw Error(errCanceled, errMsg.empty() ? "Request was canceled by unknown reason"sv : errMsg);
	}
}

class RdxDeadlineContext : public IRdxCancelContext {
public:
	using ClockT = std::chrono::steady_clock;
	using time_point = typename ClockT::time_point;
	using duration = typename ClockT::duration;

	RdxDeadlineContext(time_point deadline = time_point(), const IRdxCancelContext* parent = nullptr)
		: deadline_(deadline), parent_(parent) {}
	RdxDeadlineContext(duration timeout, const IRdxCancelContext* parent = nullptr)
		: deadline_((timeout.count() > 0) ? (ClockT::now() + timeout) : time_point()), parent_(parent) {}

	CancelType GetCancelType() const noexcept override final {
		if ((deadline_.time_since_epoch().count() > 0) &&
			(deadline_.time_since_epoch().count() < ClockT::now().time_since_epoch().count())) {
			return CancelType::Timeout;
		}
		if (parent_) {
			return parent_->GetCancelType();
		}
		return CancelType::None;
	}
	bool IsCancelable() const noexcept override final {
		return (deadline_.time_since_epoch().count() > 0) || (parent_ != nullptr && parent_->IsCancelable());
	}
	const IRdxCancelContext* parent() const noexcept { return parent_; }
	const time_point deadline() const noexcept { return deadline_; }

private:
	time_point deadline_;
	const IRdxCancelContext* parent_;
};

class RdxContext {
public:
	using Completion = std::function<void(const Error&)>;

	RdxContext(bool parallel = false)
		: holdStatus_(kEmpty), activityPtr_(nullptr), cancelCtx_(nullptr), cmpl_(nullptr), shardingParallelExecution_(parallel) {}
	RdxContext(lsn_t originLsn) : holdStatus_(kEmpty), activityPtr_(nullptr), cancelCtx_(nullptr), cmpl_(nullptr), originLsn_(originLsn) {}
	RdxContext(lsn_t originLsn, const IRdxCancelContext* cancelCtx, Completion cmpl, int emmiterServerId, bool parallel)
		: emmiterServerId_(emmiterServerId),
		  holdStatus_(kEmpty),
		  activityPtr_(nullptr),
		  cancelCtx_(cancelCtx),
		  cmpl_(cmpl),
		  originLsn_(originLsn),
		  shardingParallelExecution_(parallel) {}
	RdxContext(lsn_t originLsn, std::string_view activityTracer, std::string_view user, std::string_view query,
			   ActivityContainer& container, int connectionId, const IRdxCancelContext* cancelCtx, Completion cmpl, int emmiterServerId,
			   bool parallel)
		: emmiterServerId_(emmiterServerId),
		  holdStatus_(kHold),
		  activityCtx_(activityTracer, user, query, container, connectionId),
		  cancelCtx_(cancelCtx),
		  cmpl_(cmpl),
		  originLsn_(originLsn),
		  shardingParallelExecution_(parallel) {}
	RdxContext(RdxActivityContext* ptr, lsn_t originLsn = lsn_t(), const IRdxCancelContext* cancelCtx = nullptr, Completion cmpl = nullptr,
			   int emmiterServerId = -1, bool parallel = false)
		: emmiterServerId_(emmiterServerId),
		  holdStatus_(ptr ? kPtr : kEmpty),
		  activityPtr_(ptr),
		  cancelCtx_(cancelCtx),
		  cmpl_(cmpl),
		  originLsn_(originLsn),
		  shardingParallelExecution_(parallel) {
#ifndef NDEBUG
		if (holdStatus_ == kPtr) activityPtr_->refCount_.fetch_add(1u, std::memory_order_relaxed);
#endif
	}

	RdxContext(RdxContext&& other);
	~RdxContext();

	RdxContext(const RdxContext&) = delete;
	RdxContext& operator=(const RdxContext&) = delete;
	RdxContext& operator=(RdxContext&&) = delete;

	bool isCancelable() const noexcept { return cancelCtx_ && cancelCtx_->IsCancelable(); }
	CancelType checkCancel() const noexcept {
		if (!cancelCtx_) return CancelType::None;
		return cancelCtx_->GetCancelType();
	}
	/// returning value should be assined to a local variable which will be destroyed after the locking complete
	/// lifetime of the local variable should not exceed of the context's
	RdxActivityContext::Ward BeforeLock(MutexMark mutexMark) const;
	RdxActivityContext::Ward BeforeIndexWork() const;
	RdxActivityContext::Ward BeforeSelectLoop() const;
	/// lifetime of the returning value should not exceed of the context's
	RdxContext OnlyActivity() const { return {Activity(), originLsn_, nullptr, nullptr, -1, shardingParallelExecution_}; }
	RdxActivityContext* Activity() const noexcept;
	Completion Compl() const noexcept { return cmpl_; }
	bool NoWaitSync() const noexcept { return noWaitSync_; }
	void WithNoWaitSync(bool val = true) noexcept { noWaitSync_ = val; }
	bool HasEmmiterServer() const noexcept { return emmiterServerId_ >= 0; }
	lsn_t GetOriginLSN() const noexcept { return originLsn_; }
	bool IsShardingParallelExecution() const noexcept { return shardingParallelExecution_; }

	int emmiterServerId_ = -1;

private:
	enum { kHold, kPtr, kEmpty } const holdStatus_;
	union {
		mutable RdxActivityContext activityCtx_;
		RdxActivityContext* activityPtr_;
	};
	const IRdxCancelContext* cancelCtx_;
	Completion cmpl_;
	bool noWaitSync_ = false;
	const lsn_t originLsn_;
	bool shardingParallelExecution_ = false;
};

class QueryResults;

class InternalRdxContext {
public:
	InternalRdxContext() noexcept : cmpl_(nullptr) {}
	InternalRdxContext(const InternalRdxContext&) = default;
	InternalRdxContext(InternalRdxContext&&) = default;
	InternalRdxContext(RdxContext::Completion cmpl, const RdxDeadlineContext ctx, std::string_view activityTracer, std::string_view user,
					   int connectionId, lsn_t lsn, int emmiterServerId, int shardId, bool parallel) noexcept
		: cmpl_(cmpl),
		  deadlineCtx_(std::move(ctx)),
		  activityTracer_(activityTracer),
		  user_(user),
		  connectionId_(connectionId),
		  lsn_(lsn),
		  emmiterServerId_(emmiterServerId),
		  shardId_(shardId),
		  shardingParallelExecution_(parallel) {}

	InternalRdxContext WithCompletion(RdxContext::Completion cmpl) const noexcept {
		return InternalRdxContext(cmpl, deadlineCtx_, activityTracer_, user_, connectionId_, lsn_, emmiterServerId_, shardId_,
								  shardingParallelExecution_);
	}
	InternalRdxContext WithTimeout(milliseconds timeout) const noexcept {
		return InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()), activityTracer_, user_, connectionId_, lsn_,
								  emmiterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithLSN(lsn_t lsn) const noexcept {
		return InternalRdxContext(cmpl_, deadlineCtx_, activityTracer_, user_, connectionId_, lsn, emmiterServerId_, shardId_,
								  shardingParallelExecution_);
	}
	InternalRdxContext WithCancelParent(const IRdxCancelContext* parent) const noexcept {
		return InternalRdxContext(cmpl_, RdxDeadlineContext(deadlineCtx_.deadline(), parent), activityTracer_, user_, connectionId_, lsn_,
								  emmiterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithActivityTracer(std::string_view activityTracer, std::string_view user,
										  int connectionId = kNoConnectionId) const {
		return activityTracer.empty()
				   ? *this
				   : InternalRdxContext(cmpl_, deadlineCtx_,
										(activityTracer_.empty() ? "" : activityTracer_ + "/") + std::string(activityTracer), user,
										connectionId, lsn_, emmiterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithEmmiterServerId(unsigned int sId) const noexcept {
		return InternalRdxContext(cmpl_, deadlineCtx_, activityTracer_, user_, connectionId_, lsn_, sId, shardId_,
								  shardingParallelExecution_);
	}
	InternalRdxContext WithShardId(unsigned int id, bool parallel) const noexcept {
		return InternalRdxContext(cmpl_, deadlineCtx_, activityTracer_, user_, connectionId_, lsn_, emmiterServerId_, id, parallel);
	}
	void SetActivityTracer(std::string_view activityTracer, std::string_view user, int connectionId = kNoConnectionId) {
		activityTracer_ = std::string(activityTracer);
		user_ = std::string(user);
		connectionId_ = connectionId;
	}

	RdxContext CreateRdxContext(std::string_view query, ActivityContainer&) const;
	RdxContext CreateRdxContext(std::string_view query, ActivityContainer&, QueryResults&) const;
	RdxContext::Completion Compl() const noexcept { return cmpl_; }
	bool NeedTraceActivity() const noexcept { return !activityTracer_.empty(); }
	lsn_t LSN() const noexcept { return lsn_; }
	bool HasEmmiterID() const noexcept { return emmiterServerId_ >= 0; }
	bool HasDeadline() const noexcept { return deadlineCtx_.IsCancelable(); }
	int ShardId() const noexcept { return shardId_; }
	bool IsShardingParallelExecution() const noexcept { return shardingParallelExecution_; }
	void SetShardingParallelExecution(bool parallel) noexcept { shardingParallelExecution_ = parallel; }

	static const int kNoConnectionId = -1;

private:
	RdxContext::Completion cmpl_;
	RdxDeadlineContext deadlineCtx_;
	std::string activityTracer_;
	std::string user_;
	int connectionId_ = kNoConnectionId;
	lsn_t lsn_;
	int emmiterServerId_ = -1;
	int shardId_ = -1;
	bool shardingParallelExecution_ = false;
};

}  // namespace reindexer
