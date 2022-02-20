#include "rdxcontext.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

RdxContext::RdxContext(RdxContext&& other)
	: holdStatus_(other.holdStatus_),
	  activityPtr_(nullptr),
	  cancelCtx_(other.cancelCtx_),
	  cmpl_(other.cmpl_),
	  noWaitSync_(other.noWaitSync_),
	  originLsn_(other.originLsn_),
	  shardingParallelExecution_{other.shardingParallelExecution_} {
	if (holdStatus_ == kHold) {
		new (&activityCtx_) RdxActivityContext(std::move(other.activityCtx_));
	} else if (holdStatus_ == kPtr) {
		activityPtr_ = other.activityPtr_;
	}
}

RdxContext::~RdxContext() {
	if (holdStatus_ == kHold) {
		activityCtx_.~RdxActivityContext();
#ifndef NDEBUG
	} else if (holdStatus_ == kPtr) {
		assert(activityPtr_->refCount_.fetch_sub(1, std::memory_order_relaxed) != 0u);
#endif
	}
}

RdxActivityContext* RdxContext::Activity() const noexcept {
	switch (holdStatus_) {
		case kHold:
			return &activityCtx_;
		case kPtr:
			return activityPtr_;
		default:
			return nullptr;
	}
}

RdxActivityContext::Ward RdxContext::BeforeLock(MutexMark mutexMark) const {
	switch (holdStatus_) {
		case kHold:
			return activityCtx_.BeforeLock(mutexMark);
		case kPtr:
			return activityPtr_->BeforeLock(mutexMark);
		default:
			return RdxActivityContext::Ward{nullptr, mutexMark};
	}
}

RdxActivityContext::Ward RdxContext::BeforeIndexWork() const {
	switch (holdStatus_) {
		case kHold:
			return activityCtx_.BeforeIndexWork();
		case kPtr:
			return activityPtr_->BeforeIndexWork();
		default:
			return RdxActivityContext::Ward{nullptr, Activity::IndexesLookup};
	}
}

RdxActivityContext::Ward RdxContext::BeforeSelectLoop() const {
	switch (holdStatus_) {
		case kHold:
			return activityCtx_.BeforeSelectLoop();
		case kPtr:
			return activityPtr_->BeforeSelectLoop();
		default:
			return RdxActivityContext::Ward{nullptr, Activity::SelectLoop};
	}
}

RdxContext InternalRdxContext::CreateRdxContext(std::string_view query, ActivityContainer& activityContainer) const {
	if (activityTracer_.empty() || query.empty()) {
		return {LSN(), (deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_, emmiterServerId_, shardingParallelExecution_};
	} else {
		return {LSN(),
				activityTracer_,
				user_,
				query,
				activityContainer,
				connectionId_,
				(deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr),
				cmpl_,
				emmiterServerId_,
				shardingParallelExecution_};
	}
}

RdxContext InternalRdxContext::CreateRdxContext(std::string_view query, ActivityContainer& activityContainer,
												LocalQueryResults& qresults) const {
	if (activityTracer_.empty() || query.empty())
		return {LSN(), (deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_, emmiterServerId_, shardingParallelExecution_};
	assert(!qresults.activityCtx_);
	qresults.activityCtx_.emplace(activityTracer_, user_, query, activityContainer, connectionId_, true);
	return {&*(qresults.activityCtx_), LSN(), (deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_, emmiterServerId_,
			shardingParallelExecution_};
}

}  // namespace reindexer
