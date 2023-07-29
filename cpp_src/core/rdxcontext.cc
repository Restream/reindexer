#include "rdxcontext.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

RdxContext::RdxContext(RdxContext&& other) noexcept
	: fromReplication_(other.fromReplication_),
	  LSNs_(other.LSNs_),
	  holdStatus_(other.holdStatus_),
	  activityPtr_(nullptr),
	  cancelCtx_(other.cancelCtx_),
	  cmpl_(other.cmpl_) {
	if (holdStatus_ == kHold) {
		new (&activityCtx_) RdxActivityContext(std::move(other.activityCtx_));
		other.activityCtx_.~RdxActivityContext();
		other.holdStatus_ = kEmpty;
	} else if (holdStatus_ == kPtr) {
		activityPtr_ = other.activityPtr_;
		other.holdStatus_ = kEmpty;
	}
}

RdxContext::~RdxContext() {
	if (holdStatus_ == kHold) {
		activityCtx_.~RdxActivityContext();
	} else if (holdStatus_ == kPtr) {
		[[maybe_unused]] const auto refs = activityPtr_->refCount_.fetch_sub(1, std::memory_order_relaxed);
		assertrx(refs != 0u);
	}
}

RdxActivityContext* RdxContext::Activity() const noexcept {
	switch (holdStatus_) {
		case kHold:
			return &activityCtx_;
		case kPtr:
			return activityPtr_;
		case kEmpty:
		default:
			return nullptr;
	}
}

RdxActivityContext::Ward RdxContext::BeforeLock(MutexMark mutexMark) const noexcept {
	switch (holdStatus_) {
		case kHold:
			return activityCtx_.BeforeLock(mutexMark);
		case kPtr:
			return activityPtr_->BeforeLock(mutexMark);
		case kEmpty:
		default:
			return RdxActivityContext::Ward{nullptr, mutexMark};
	}
}

RdxActivityContext::Ward RdxContext::BeforeIndexWork() const noexcept {
	switch (holdStatus_) {
		case kHold:
			return activityCtx_.BeforeIndexWork();
		case kPtr:
			return activityPtr_->BeforeIndexWork();
		case kEmpty:
		default:
			return RdxActivityContext::Ward{nullptr, Activity::IndexesLookup};
	}
}

RdxActivityContext::Ward RdxContext::BeforeSelectLoop() const noexcept {
	switch (holdStatus_) {
		case kHold:
			return activityCtx_.BeforeSelectLoop();
		case kPtr:
			return activityPtr_->BeforeSelectLoop();
		case kEmpty:
		default:
			return RdxActivityContext::Ward{nullptr, Activity::SelectLoop};
	}
}

RdxContext InternalRdxContext::CreateRdxContext(std::string_view query, ActivityContainer& activityContainer) const {
	if (activityTracer_.empty() || query.empty()) {
		return {(deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_};
	} else {
		return {activityTracer_,
				user_,
				query,
				activityContainer,
				connectionId_,
				(deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr),
				cmpl_};
	}
}

RdxContext InternalRdxContext::CreateRdxContext(std::string_view query, ActivityContainer& activityContainer,
												QueryResults& qresults) const {
	if (activityTracer_.empty() || query.empty()) return {(deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_};
	assertrx(!qresults.activityCtx_);
	qresults.activityCtx_.emplace(activityTracer_, user_, query, activityContainer, connectionId_, true);
	return RdxContext{&*(qresults.activityCtx_), (deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_};
}

}  // namespace reindexer
