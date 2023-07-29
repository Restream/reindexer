#pragma once

#include <functional>
#include "activity_context.h"
#include "lsn.h"
#include "tools/errors.h"

namespace reindexer {

using std::chrono::milliseconds;

enum class CancelType : uint8_t { None = 0, Explicit, Timeout };

struct IRdxCancelContext {
	virtual CancelType GetCancelType() const noexcept = 0;
	virtual bool IsCancelable() const noexcept = 0;

	virtual ~IRdxCancelContext() = default;
};

template <typename Context>
void ThrowOnCancel(const Context& ctx, std::string_view errMsg = std::string_view()) {	// TODO may be ""sv
	if (!ctx.isCancelable()) return;

	auto cancel = ctx.checkCancel();
	switch (cancel) {
		case CancelType::Explicit:
			throw Error(errCanceled, errMsg);
		case CancelType::Timeout:
			throw Error(errTimeout, errMsg);
		case CancelType::None:
			return;
		default:
			assertrx(false);
			throw Error(errCanceled, errMsg);
	}
}

class RdxDeadlineContext : public IRdxCancelContext {
public:
	using ClockT = std::chrono::steady_clock;
	using time_point = typename ClockT::time_point;
	using duration = typename ClockT::duration;

	RdxDeadlineContext(time_point deadline = time_point(), const IRdxCancelContext* parent = nullptr) noexcept
		: deadline_(deadline), parent_(parent) {}
	RdxDeadlineContext(duration timeout, const IRdxCancelContext* parent = nullptr) noexcept
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
	time_point deadline() const noexcept { return deadline_; }

private:
	time_point deadline_;
	const IRdxCancelContext* parent_;
};

class RdxContext {
public:
	using Completion = std::function<void(const Error&)>;

	RdxContext() noexcept : fromReplication_(false), holdStatus_(kEmpty), activityPtr_(nullptr), cancelCtx_(nullptr), cmpl_(nullptr) {}
	RdxContext(bool fromReplication, const LSNPair& LSNs) noexcept
		: fromReplication_(fromReplication), LSNs_(LSNs), holdStatus_(kEmpty), activityPtr_(nullptr), cancelCtx_(nullptr), cmpl_(nullptr) {}

	RdxContext(const IRdxCancelContext* cancelCtx, Completion cmpl) noexcept
		: fromReplication_(false), holdStatus_(kEmpty), activityPtr_(nullptr), cancelCtx_(cancelCtx), cmpl_(std::move(cmpl)) {}
	RdxContext(std::string_view activityTracer, std::string_view user, std::string_view query, ActivityContainer& container,
			   int connectionId, const IRdxCancelContext* cancelCtx, Completion cmpl)
		: fromReplication_(false),
		  holdStatus_(kHold),
		  activityCtx_(activityTracer, user, query, container, connectionId),
		  cancelCtx_(cancelCtx),
		  cmpl_(std::move(cmpl)) {}
	explicit RdxContext(RdxActivityContext* ptr, const IRdxCancelContext* cancelCtx = nullptr, Completion cmpl = nullptr) noexcept
		: fromReplication_(false), holdStatus_(ptr ? kPtr : kEmpty), activityPtr_(ptr), cancelCtx_(cancelCtx), cmpl_(std::move(cmpl)) {
		if (holdStatus_ == kPtr) activityPtr_->refCount_.fetch_add(1u, std::memory_order_relaxed);
	}

	RdxContext(RdxContext&& other) noexcept;
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
	RdxActivityContext::Ward BeforeLock(MutexMark mutexMark) const noexcept;
	RdxActivityContext::Ward BeforeIndexWork() const noexcept;
	RdxActivityContext::Ward BeforeSelectLoop() const noexcept;
	/// lifetime of the returning value should not exceed of the context's
	RdxContext OnlyActivity() const { return RdxContext{Activity()}; }
	RdxActivityContext* Activity() const noexcept;
	Completion Compl() const { return cmpl_; }

	const bool fromReplication_;
	LSNPair LSNs_;

private:
	enum { kHold, kPtr, kEmpty } holdStatus_;
	union {
		mutable RdxActivityContext activityCtx_;
		RdxActivityContext* activityPtr_;
	};
	const IRdxCancelContext* cancelCtx_;
	Completion cmpl_;
};

class QueryResults;

class InternalRdxContext {
public:
	InternalRdxContext() noexcept {}
	InternalRdxContext(RdxContext::Completion cmpl, RdxDeadlineContext ctx, std::string activityTracer, std::string user,
					   int connectionId) noexcept
		: cmpl_(std::move(cmpl)),
		  deadlineCtx_(std::move(ctx)),
		  activityTracer_(std::move(activityTracer)),
		  user_(std::move(user)),
		  connectionId_(connectionId) {}

	InternalRdxContext WithCompletion(RdxContext::Completion cmpl) const noexcept {
		return InternalRdxContext(std::move(cmpl), deadlineCtx_, activityTracer_, user_, connectionId_);
	}
	InternalRdxContext WithTimeout(milliseconds timeout) const noexcept {
		return InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()), activityTracer_, user_, connectionId_);
	}
	InternalRdxContext WithCancelParent(const IRdxCancelContext* parent) const noexcept {
		return InternalRdxContext(cmpl_, RdxDeadlineContext(deadlineCtx_.deadline(), parent), activityTracer_, user_, connectionId_);
	}
	InternalRdxContext WithActivityTracer(std::string_view activityTracer, std::string&& user, int connectionId = kNoConnectionId) const {
		return activityTracer.empty()
				   ? *this
				   : InternalRdxContext(cmpl_, deadlineCtx_,
										activityTracer_.empty() ? std::string(activityTracer)
																: std::string(activityTracer_).append("/").append(activityTracer),
										std::move(user), connectionId);
	}
	InternalRdxContext WithContextParams(milliseconds timeout, std::string_view activityTracer, std::string&& user,
										  int connectionId) const {
		return activityTracer.empty()
				   ? InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()), activityTracer_, user_, connectionId_)
				   : InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()),
										activityTracer_.empty() ? std::string(activityTracer)
																: std::string(activityTracer_).append("/").append(activityTracer),
										std::move(user), connectionId);
	}
	void SetActivityTracer(std::string&& activityTracer, std::string&& user, int connectionId = kNoConnectionId) noexcept {
		activityTracer_ = std::move(activityTracer);
		user_ = std::move(user);
		connectionId_ = connectionId;
	}

	RdxContext CreateRdxContext(std::string_view query, ActivityContainer&) const;
	RdxContext CreateRdxContext(std::string_view query, ActivityContainer&, QueryResults&) const;
	RdxContext::Completion Compl() const { return cmpl_; }
	bool NeedTraceActivity() const { return !activityTracer_.empty(); }

	static const int kNoConnectionId = -1;

private:
	RdxContext::Completion cmpl_;
	RdxDeadlineContext deadlineCtx_;
	std::string activityTracer_;
	std::string user_;
	int connectionId_ = kNoConnectionId;
};

}  // namespace reindexer
