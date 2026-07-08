#include "activity_context.h"
#include "activity_container.h"

#ifdef RX_LOGACTIVITY
#include "activity_log.h"
#endif

namespace reindexer {

using namespace std::string_view_literals;

RdxActivityContext::RdxActivityContext(std::string_view activityTracer, std::string_view user, std::string_view query,
									   ActivityContainer& parent, int ipConnectionId, bool clientState)
	: data_{nextId(),			ipConnectionId,		   std::string(activityTracer), std::string(user),
			std::string(query), system_clock_w::now(), Activity::InProgress,		""sv},
	  state_(serializeState(clientState ? Activity::Sending : Activity::InProgress)),
	  parent_(&parent) {
	parent_->Register(this);
}

// NOLINTNEXTLINE (performance-noexcept-move-constructor)
RdxActivityContext::RdxActivityContext(RdxActivityContext&& other)
	: data_(other.data_), state_(other.state_.load(std::memory_order_relaxed)), parent_(other.parent_) {
	if (parent_) {
		parent_->Reregister(&other, this);
	}
	other.parent_ = nullptr;
}

RdxActivityContext::~RdxActivityContext() {
	if (parent_) {
		parent_->Unregister(this);
	}
	assertrx(refCount_.load(std::memory_order_relaxed) == 0u);
}

RdxActivityContext::operator Activity() const {
	Activity ret = data_;
	const auto state = deserializeState(state_.load(std::memory_order_relaxed));
	ret.state = state.first;
	ret.description = state.second;
	return ret;
}

std::pair<Activity::State, std::string_view> RdxActivityContext::deserializeState(unsigned state) {
	const Activity::State decodedState = static_cast<Activity::State>(state & kStateMask);
	return decodedState == Activity::WaitLock
			   ? std::make_pair(decodedState, DescribeMutexMark(static_cast<MutexMark>(state >> kStateShift)))
			   : std::make_pair(decodedState, "");
}

unsigned RdxActivityContext::nextId() noexcept {
	static std::atomic<unsigned> idCounter{0u};
	return idCounter.fetch_add(1u, std::memory_order_relaxed);
}

RdxActivityContext::Ward::Ward(RdxActivityContext *cont, Activity::State state) noexcept : context_(cont) {
	if (context_) {
		prevState_ = context_->state_.exchange(serializeState(state), std::memory_order_relaxed);
#ifndef NDEBUG
		context_->refCount_.fetch_add(1u, std::memory_order_relaxed);
#endif
#ifdef RX_LOGACTIVITY
		if (context_->parent_) {
			context_->parent_->AddOperation(context_, state, true);
		}
#endif
	}
}

RdxActivityContext::Ward::Ward(RdxActivityContext *cont, MutexMark mutexMark) noexcept : context_(cont) {
	if (context_) {
		prevState_ = context_->state_.exchange(serializeState(mutexMark), std::memory_order_relaxed);
#ifndef NDEBUG
		context_->refCount_.fetch_add(1u, std::memory_order_relaxed);
#endif
#ifdef RX_LOGACTIVITY
		if (context_->parent_) {
			context_->parent_->AddOperation(context_, Activity::WaitLock, true);
		}
#endif
	}
}

RdxActivityContext::Ward::~Ward() {
	if (context_) {
#ifdef RX_LOGACTIVITY
		auto [state, mark] = deserializeState(context_->state_);
		(void)mark;
#endif

		context_->state_.store(prevState_, std::memory_order_relaxed);
		[[maybe_unused]] const auto refs = context_->refCount_.fetch_sub(1u, std::memory_order_relaxed);
		assertrx(refs != 0u);
#ifdef RX_LOGACTIVITY
		if (context_->parent_) {
			context_->parent_->AddOperation(context_, state, false);
		}
#endif
	}
}

}  // namespace reindexer
