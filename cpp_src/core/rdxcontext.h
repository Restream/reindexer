#pragma once

#include <functional>
#include <optional>
#include "activity_context.h"
#include "tools/errors.h"
#include "tools/lsn.h"

namespace reindexer {

using std::chrono::milliseconds;

enum class CancelType : uint8_t { None = 0, Explicit, Timeout };

struct IRdxCancelContext {
	virtual CancelType GetCancelType() const noexcept = 0;
	virtual bool IsCancelable() const noexcept = 0;
	virtual std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept = 0;

	virtual ~IRdxCancelContext() = default;
};

constexpr std::string_view kDefaultTimeoutError = "Context timeout";
constexpr std::string_view kDefaultCancelError = "Context was canceled";

template <typename Context>
void ThrowOnCancel(const Context& ctx, std::string_view errMsg = std::string_view()) {
	if (!ctx.IsCancelable()) return;

	const auto cancel = ctx.CheckCancel();
	switch (cancel) {
		case CancelType::Explicit:
			throw Error(errCanceled, errMsg.empty() ? kDefaultCancelError : errMsg);
		case CancelType::Timeout:
			throw Error(errTimeout, errMsg.empty() ? kDefaultTimeoutError : errMsg);
		case CancelType::None:
			return;
	}
	assertrx(false);
	throw Error(errCanceled, errMsg.empty() ? kDefaultCancelError : errMsg);
}

class RdxDeadlineContext : public IRdxCancelContext {
public:
	using ClockT = std::chrono::steady_clock;
	using time_point = typename ClockT::time_point;

	RdxDeadlineContext(time_point deadline = time_point(), const IRdxCancelContext* parent = nullptr) noexcept
		: deadline_(deadline), parent_(parent) {}
	RdxDeadlineContext(std::chrono::milliseconds timeout, const IRdxCancelContext* parent = nullptr) noexcept
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
	CancelType CheckCancel() const noexcept { return GetCancelType(); }
	const IRdxCancelContext* parent() const noexcept { return parent_; }
	time_point deadline() const noexcept { return deadline_; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override {
		std::optional<std::chrono::milliseconds> ret;
		if (parent_) {
			ret = parent_->GetRemainingTimeout();
		}
		if (deadline_.time_since_epoch().count() > 0) {
			const auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(deadline_ - ClockT::now());
			if (diff.count() <= 0) {
				ret = std::chrono::milliseconds(-1);
			} else if (!ret.has_value() || *ret > diff) {
				ret = diff;
			}
		}
		return ret;
	}

private:
	time_point deadline_;
	const IRdxCancelContext* parent_;
};

class RdxContext {
public:
	using Completion = std::function<void(const Error&)>;

	RdxContext() noexcept : activityPtr_(nullptr), cancelCtx_(nullptr), cmpl_(nullptr), holdStatus_(HoldT::kEmpty) {}
	explicit RdxContext(lsn_t originLsn) noexcept
		: activityPtr_(nullptr), cancelCtx_(nullptr), cmpl_(nullptr), originLsn_(originLsn), holdStatus_(HoldT::kEmpty) {}
	RdxContext(lsn_t originLsn, const IRdxCancelContext* cancelCtx, Completion cmpl, int emmiterServerId, int shardId,
			   bool parallel) noexcept
		: activityPtr_(nullptr),
		  cancelCtx_(cancelCtx),
		  cmpl_(cmpl),
		  originLsn_(originLsn),
		  emmiterServerId_(emmiterServerId),
		  shardId_(shardId),
		  holdStatus_(HoldT::kEmpty),
		  shardingParallelExecution_(parallel) {}
	RdxContext(lsn_t originLsn, std::string_view activityTracer, std::string_view user, std::string_view query,
			   ActivityContainer& container, int connectionId, const IRdxCancelContext* cancelCtx, Completion cmpl, int emmiterServerId,
			   int shardId, bool parallel)
		: activityCtx_(activityTracer, user, query, container, connectionId),
		  cancelCtx_(cancelCtx),
		  cmpl_(cmpl),
		  originLsn_(originLsn),
		  emmiterServerId_(emmiterServerId),
		  shardId_(shardId),
		  holdStatus_(HoldT::kHold),
		  shardingParallelExecution_(parallel) {}
	explicit RdxContext(RdxActivityContext* ptr, lsn_t originLsn = lsn_t(), const IRdxCancelContext* cancelCtx = nullptr,
						Completion cmpl = nullptr, int emmiterServerId = -1, int shardId = ShardingKeyType::NotSetShard,
						bool parallel = false, bool noWaitSync = false) noexcept
		: activityPtr_(ptr),
		  cancelCtx_(cancelCtx),
		  cmpl_(cmpl),
		  originLsn_(originLsn),
		  emmiterServerId_(emmiterServerId),
		  shardId_(shardId),
		  holdStatus_(ptr ? HoldT::kPtr : HoldT::kEmpty),
		  shardingParallelExecution_(parallel),
		  noWaitSync_(noWaitSync) {
		if (holdStatus_ == HoldT::kPtr) activityPtr_->refCount_.fetch_add(1u, std::memory_order_relaxed);
	}

	RdxContext(RdxContext&& other) noexcept;
	~RdxContext();

	RdxContext(const RdxContext&) = delete;
	RdxContext& operator=(const RdxContext&) = delete;
	RdxContext& operator=(RdxContext&&) = delete;

	const IRdxCancelContext* GetCancelCtx() const noexcept { return cancelCtx_; }
	bool IsCancelable() const noexcept { return cancelCtx_ && cancelCtx_->IsCancelable(); }
	CancelType CheckCancel() const noexcept {
		if (!cancelCtx_) return CancelType::None;
		return cancelCtx_->GetCancelType();
	}
	/// returning value should be assined to a local variable which will be destroyed after the locking complete
	/// lifetime of the local variable should not exceed of the context's
	RdxActivityContext::Ward BeforeLock(MutexMark mutexMark) const noexcept;
	RdxActivityContext::Ward BeforeIndexWork() const noexcept;
	RdxActivityContext::Ward BeforeSelectLoop() const noexcept;
	RdxActivityContext::Ward BeforeClusterProxy() const noexcept;
	RdxActivityContext::Ward BeforeShardingProxy() const noexcept;
	RdxActivityContext::Ward BeforeSimpleState(Activity::State st) const noexcept;

	/// lifetime of the returning value should not exceed of the context's
	RdxContext OnlyActivity() const { return RdxContext{Activity(), originLsn_, nullptr, nullptr, -1, shardingParallelExecution_}; }
	RdxActivityContext* Activity() const noexcept;
	Completion Compl() const noexcept { return cmpl_; }
	bool NoWaitSync() const noexcept { return noWaitSync_; }
	void WithNoWaitSync(bool val = true) const noexcept { noWaitSync_ = val; }
	bool HasEmmiterServer() const noexcept { return emmiterServerId_ >= 0; }
	lsn_t GetOriginLSN() const noexcept { return originLsn_; }
	bool IsShardingParallelExecution() const noexcept { return shardingParallelExecution_; }
	int EmmiterServerId() const noexcept { return emmiterServerId_; }
	int ShardId() const noexcept { return shardId_; }
	RdxContext WithCancelCtx(const IRdxCancelContext& cancelCtx) const {
		return RdxContext{
			Activity(), originLsn_, &cancelCtx, cmpl_, emmiterServerId_, shardingParallelExecution_, shardingParallelExecution_,
			noWaitSync_};
	}
	RdxContext WithShardId(int shardId, bool shardingParallelExecution) const {
		return RdxContext{Activity(), originLsn_, cancelCtx_, cmpl_, emmiterServerId_, shardId, shardingParallelExecution, noWaitSync_};
	}
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept {
		if (cancelCtx_) {
			return cancelCtx_->GetRemainingTimeout();
		}
		return std::nullopt;
	}

private:
	union {
		mutable RdxActivityContext activityCtx_;
		RdxActivityContext* activityPtr_;
	};
	const IRdxCancelContext* cancelCtx_;
	Completion cmpl_;
	const lsn_t originLsn_;
	int emmiterServerId_ = -1;
	int shardId_ = ShardingKeyType::NotSetShard;
	enum class HoldT : uint8_t { kHold, kPtr, kEmpty } holdStatus_;
	bool shardingParallelExecution_ = false;
	mutable bool noWaitSync_ = false;  // FIXME: Create SyncContext and move this parameter into it
};

class QueryResults;

class InternalRdxContext {
public:
	InternalRdxContext() noexcept {}
	InternalRdxContext(const InternalRdxContext&) = default;
	InternalRdxContext(InternalRdxContext&&) = default;
	InternalRdxContext(RdxContext::Completion cmpl, const RdxDeadlineContext ctx, std::string activityTracer, std::string user,
					   int connectionId, lsn_t lsn, int emmiterServerId, int shardId, bool parallel) noexcept
		: cmpl_(std::move(cmpl)),
		  deadlineCtx_(std::move(ctx)),
		  activityTracer_(std::move(activityTracer)),
		  user_(std::move(user)),
		  connectionId_(connectionId),
		  emmiterServerId_(emmiterServerId),
		  lsn_(lsn),
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
	InternalRdxContext WithActivityTracer(std::string_view activityTracer, std::string&& user, int connectionId = kNoConnectionId) const {
		return activityTracer.empty()
				   ? *this
				   : InternalRdxContext(cmpl_, deadlineCtx_,
										activityTracer_.empty() ? std::string(activityTracer)
																: std::string(activityTracer_).append("/").append(activityTracer),
										std::move(user), connectionId, lsn_, emmiterServerId_, shardId_, shardingParallelExecution_);
	}
	InternalRdxContext WithEmmiterServerId(unsigned int sId) const noexcept {
		return InternalRdxContext(cmpl_, deadlineCtx_, activityTracer_, user_, connectionId_, lsn_, sId, shardId_,
								  shardingParallelExecution_);
	}
	InternalRdxContext WithShardId(unsigned int id, bool parallel) const noexcept {
		return InternalRdxContext(cmpl_, deadlineCtx_, activityTracer_, user_, connectionId_, lsn_, emmiterServerId_, id, parallel);
	}
	InternalRdxContext WithContextParams(milliseconds timeout, lsn_t lsn, int emmiterServerId, int shardId, bool distributed) const {
		return InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()), activityTracer_, user_, connectionId_, lsn,
								  emmiterServerId, shardId, distributed);
	}
	InternalRdxContext WithContextParams(milliseconds timeout, lsn_t lsn, int emmiterServerId, int shardId, bool distributed,
										 std::string_view activityTracer, std::string&& user, int connectionId) const {
		return activityTracer.empty()
				   ? InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()), activityTracer_, user_, connectionId_,
										lsn, emmiterServerId, shardId, distributed)
				   : InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()),
										activityTracer_.empty() ? std::string(activityTracer)
																: std::string(activityTracer_).append("/").append(activityTracer),
										std::move(user), connectionId, lsn, emmiterServerId, shardId, distributed);
	}
	InternalRdxContext WithContextParams(milliseconds timeout, std::string_view activityTracer, std::string&& user) const {
		return activityTracer.empty()
				   ? InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()), activityTracer_, user_, connectionId_,
										lsn_, emmiterServerId_, shardId_, shardingParallelExecution_)
				   : InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()),
										activityTracer_.empty() ? std::string(activityTracer)
																: std::string(activityTracer_).append("/").append(activityTracer),
										std::move(user), connectionId_, lsn_, emmiterServerId_, shardId_, shardingParallelExecution_);
	}
	void SetActivityTracer(std::string&& activityTracer, std::string&& user, int connectionId = kNoConnectionId) noexcept {
		activityTracer_ = std::move(activityTracer);
		user_ = std::move(user);
		connectionId_ = connectionId;
	}

	RdxContext CreateRdxContext(std::string_view query, ActivityContainer&) const;
	RdxContext CreateRdxContext(std::string_view query, ActivityContainer&, QueryResults&) const;
	RdxContext::Completion Compl() const noexcept { return cmpl_; }
	bool NeedTraceActivity() const noexcept { return !activityTracer_.empty(); }
	lsn_t LSN() const noexcept { return lsn_; }
	bool HasEmmiterID() const noexcept { return emmiterServerId_ >= 0; }
	bool HasDeadline() const noexcept { return deadlineCtx_.IsCancelable(); }
	const RdxDeadlineContext* DeadlineCtx() const noexcept { return &deadlineCtx_; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept { return deadlineCtx_.GetRemainingTimeout(); }
	int ShardId() const noexcept { return shardId_; }
	bool IsShardingParallelExecution() const noexcept { return shardingParallelExecution_; }
	void SetShardingParallelExecution(bool parallel) noexcept { shardingParallelExecution_ = parallel; }
	void SetTimeout(std::chrono::seconds timeout) noexcept {
		const auto deadline = std::chrono::steady_clock::now() + timeout;
		const auto curDeadlineCnt = deadlineCtx_.deadline().time_since_epoch().count();
		if (curDeadlineCnt > 0) {
			const auto newDeadlineCnt = deadline.time_since_epoch().count();
			deadlineCtx_ = RdxDeadlineContext(newDeadlineCnt < curDeadlineCnt ? deadline : deadlineCtx_.deadline(), deadlineCtx_.parent());
		} else {
			deadlineCtx_ = RdxDeadlineContext(deadline, deadlineCtx_.parent());
		}
	}

	static const int kNoConnectionId = -1;

private:
	RdxContext::Completion cmpl_;
	RdxDeadlineContext deadlineCtx_;
	std::string activityTracer_;
	std::string user_;
	int connectionId_ = kNoConnectionId;
	int emmiterServerId_ = -1;
	lsn_t lsn_;
	int shardId_ = ShardingKeyType::NotSetShard;
	bool shardingParallelExecution_ = false;
};

}  // namespace reindexer
