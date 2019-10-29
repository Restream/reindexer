#pragma once

#include "activity_context.h"
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
void ThrowOnCancel(const Context& ctx, string_view errMsg = string_view()) {
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
			assert(false);
			throw Error(errCanceled, errMsg);
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

	RdxContext() : holdStatus_(kEmpty), activityPtr_(nullptr), cancelCtx_(nullptr), cmpl_(nullptr) {}
	RdxContext(const IRdxCancelContext* cancelCtx, Completion cmpl)
		: holdStatus_(kEmpty), activityPtr_(nullptr), cancelCtx_(cancelCtx), cmpl_(cmpl) {}
	RdxContext(string_view activityTracer, string_view user, string_view query, ActivityContainer& container,
			   const IRdxCancelContext* cancelCtx, Completion cmpl)
		: holdStatus_(kHold), activityCtx_(activityTracer, user, query, container), cancelCtx_(cancelCtx), cmpl_(cmpl) {}
	RdxContext(RdxActivityContext* ptr, const IRdxCancelContext* cancelCtx = nullptr, Completion cmpl = nullptr)
		: holdStatus_(ptr ? kPtr : kEmpty), activityPtr_(ptr), cancelCtx_(cancelCtx), cmpl_(cmpl) {
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
	RdxContext OnlyActivity() const { return {Activity()}; }
	RdxActivityContext* Activity() const;
	Completion Compl() const { return cmpl_; }

private:
	enum { kHold, kPtr, kEmpty } const holdStatus_;
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
	InternalRdxContext() noexcept : cmpl_(nullptr) {}
	InternalRdxContext(RdxContext::Completion cmpl, const RdxDeadlineContext ctx, string_view activityTracer, string_view user) noexcept
		: cmpl_(cmpl), deadlineCtx_(std::move(ctx)), activityTracer_(activityTracer), user_(user) {}

	InternalRdxContext WithCompletion(RdxContext::Completion cmpl) const noexcept {
		return InternalRdxContext(cmpl, deadlineCtx_, activityTracer_, user_);
	}
	InternalRdxContext WithTimeout(milliseconds timeout) const noexcept {
		return InternalRdxContext(cmpl_, RdxDeadlineContext(timeout, deadlineCtx_.parent()), activityTracer_, user_);
	}
	InternalRdxContext WithCancelParent(const IRdxCancelContext* parent) const noexcept {
		return InternalRdxContext(cmpl_, RdxDeadlineContext(deadlineCtx_.deadline(), parent), activityTracer_, user_);
	}
	InternalRdxContext WithActivityTracer(string_view activityTracer, string_view user) const {
		return activityTracer.empty()
				   ? *this
				   : InternalRdxContext(cmpl_, deadlineCtx_,
										(activityTracer_.empty() ? "" : activityTracer_ + "/") + string(activityTracer), user);
	}
	void SetActivityTracer(string_view activityTracer, string_view user) {
		activityTracer_ = string(activityTracer);
		user_ = string(user);
	}

	RdxContext CreateRdxContext(string_view query, ActivityContainer&) const;
	RdxContext CreateRdxContext(string_view query, ActivityContainer&, QueryResults&) const;
	RdxContext::Completion Compl() const { return cmpl_; }
	bool NeedTraceActivity() const { return !activityTracer_.empty(); }

private:
	RdxContext::Completion cmpl_;
	RdxDeadlineContext deadlineCtx_;
	string activityTracer_;
	string user_;
};

}  // namespace reindexer
