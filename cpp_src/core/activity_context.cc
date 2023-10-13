#include "activity_context.h"
#include "activitylog.h"
#include "tools/stringstools.h"

namespace reindexer {

using namespace std::string_view_literals;

void ActivityContainer::Register(const RdxActivityContext* context) {
	std::unique_lock lck(mtx_);
	const auto res = cont_.insert(context);
	lck.unlock();

	assertrx(res.second);
	(void)res;
#ifdef RX_LOGACTIVITY
	log_.Register(context);
#endif
}

void ActivityContainer::Unregister(const RdxActivityContext* context) {
	std::unique_lock lck(mtx_);
	const auto count = cont_.erase(context);
	lck.unlock();

	assertrx(count == 1u);
	(void)count;
#ifdef RX_LOGACTIVITY
	log_.Unregister(context);
#endif
}

void ActivityContainer::Reregister(const RdxActivityContext* oldCtx, const RdxActivityContext* newCtx) {
	if (oldCtx == newCtx) return;

	std::unique_lock lck(mtx_);
	const auto eraseCount = cont_.erase(oldCtx);
	const auto insertRes = cont_.insert(newCtx);
	lck.unlock();

	assertrx(eraseCount == 1u);
	assertrx(insertRes.second);
	(void)eraseCount;
	(void)insertRes;
#ifdef RX_LOGACTIVITY
	log_.Reregister(oldCtx, newCtx);
#endif
}

void ActivityContainer::Reset() {
#ifdef RX_LOGACTIVITY
	std::lock_guard lck(mtx_);
	log_.Reset();
#endif
}

#ifdef RX_LOGACTIVITY
void ActivityContainer::AddOperation(const RdxActivityContext* ctx, Activity::State st, bool start) {
	std::unique_lock<std::mutex> lck(mtx_);
	log_.AddOperation(ctx, st, start);
}
#endif

std::vector<Activity> ActivityContainer::List([[maybe_unused]] int serverId) {
	std::vector<Activity> ret;
	{
		std::lock_guard lck(mtx_);

#ifdef RX_LOGACTIVITY
		log_.Dump(serverId);
#endif

		ret.reserve(cont_.size());
		for (const RdxActivityContext* ctx : cont_) ret.emplace_back(*ctx);
	}
	return ret;
}

std::optional<std::string> ActivityContainer::QueryForIpConnection(int id) {
	std::lock_guard lck(mtx_);

	for (const RdxActivityContext* ctx : cont_) {
		if (ctx->CheckConnectionId(id)) {
			std::string ret;
			deepCopy(ret, ctx->Query());
			return std::optional{std::move(ret)};
		}
	}

	return std::nullopt;
}

RdxActivityContext::RdxActivityContext(std::string_view activityTracer, std::string_view user, std::string_view query,
									   ActivityContainer& parent, int ipConnectionId, bool clientState)
	: data_{nextId(),
			ipConnectionId,
			std::string(activityTracer),
			std::string(user),
			std::string(query),
			std::chrono::system_clock::now(),
			Activity::InProgress,
			""sv},
	  state_(serializeState(clientState ? Activity::Sending : Activity::InProgress)),
#ifndef NDEBUG
	  refCount_(0u),
#endif
	  parent_(&parent)

{
	parent_->Register(this);
}

// NOLINTNEXTLINE (performance-noexcept-move-constructor)
RdxActivityContext::RdxActivityContext(RdxActivityContext&& other)
	: data_(other.data_),
	  state_(other.state_.load(std::memory_order_relaxed)),
#ifndef NDEBUG
	  refCount_(0u),
#endif
	  parent_(other.parent_) {
	if (parent_) parent_->Reregister(&other, this);
	other.parent_ = nullptr;
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

}  // namespace reindexer
