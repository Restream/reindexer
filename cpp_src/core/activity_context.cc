#include "activity_context.h"
#include <iomanip>
#include <sstream>
#include "cjson/jsonbuilder.h"

namespace reindexer {

void ActivityContainer::Register(const RdxActivityContext* context) {
	std::unique_lock<std::mutex> lck(mtx_);
	const auto res = cont_.insert(context);
	assert(res.second);
	(void)res;
}

void ActivityContainer::Unregister(const RdxActivityContext* context) {
	std::unique_lock<std::mutex> lck(mtx_);
	const auto count = cont_.erase(context);
	assert(count == 1u);
	(void)count;
}

void ActivityContainer::Reregister(const RdxActivityContext* oldCtx, const RdxActivityContext* newCtx) {
	if (oldCtx == newCtx) return;
	std::unique_lock<std::mutex> lck(mtx_);
	const auto eraseCount = cont_.erase(oldCtx);
	assert(eraseCount == 1u);
	const auto insertRes = cont_.insert(newCtx);
	assert(insertRes.second);
	(void)eraseCount;
	(void)insertRes;
}

std::vector<Activity> ActivityContainer::List() {
	std::vector<Activity> ret;
	std::unique_lock<std::mutex> lck(mtx_);
	ret.reserve(cont_.size());
	for (const RdxActivityContext* ctx : cont_) ret.push_back(*ctx);
	return ret;
}

string_view Activity::DescribeState(State st) {
	switch (st) {
		case InProgress:
			return "in_progress";
		case WaitLock:
			return "wait_lock";
		case Sending:
			return "sending";
		case IndexesLookup:
			return "indexes_lookup";
		case SelectLoop:
			return "select_loop";
		default:
			abort();
	}
}

void Activity::GetJSON(WrSerializer& ser) const {
	using namespace std::chrono;
	JsonBuilder builder(ser);
	builder.Put("client", activityTracer);
	if (!user.empty()) builder.Put("user", user);
	builder.Put("query", query);
	builder.Put("query_id", id);
	std::time_t t = system_clock::to_time_t(startTime);
	char buffer[80];
	std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
	std::stringstream ss;
	ss << buffer << '.' << std::setw(3) << std::setfill('0') << (duration_cast<milliseconds>(startTime.time_since_epoch()).count() % 1000);
	builder.Put("query_start", ss.str());
	builder.Put("state", DescribeState(state));
	if (state == WaitLock) builder.Put("lock_description", "Wait lock for " + string(description));
	builder.End();
}

RdxActivityContext::RdxActivityContext(string_view activityTracer, string_view user, string_view query, ActivityContainer& parent,
									   bool clientState)
	: data_{nextId(), string(activityTracer), string(user), string(query), std::chrono::system_clock::now(), Activity::InProgress, ""_sv},
	  state_(serializeState(clientState ? Activity::Sending : Activity::InProgress)),
	  parent_(&parent)
#ifndef NDEBUG
	  ,
	  refCount_(0u)
#endif
{
	parent_->Register(this);
}

RdxActivityContext::RdxActivityContext(RdxActivityContext&& other)
	: data_(other.data_),
	  state_(other.state_.load(std::memory_order_relaxed)),
	  parent_(other.parent_)
#ifndef NDEBUG
	  ,
	  refCount_(0u)
#endif
{
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

unsigned RdxActivityContext::serializeState(MutexMark mark) { return Activity::WaitLock | (static_cast<unsigned>(mark) << kStateShift); }
unsigned RdxActivityContext::serializeState(Activity::State state) { return static_cast<unsigned>(state); }

std::pair<Activity::State, string_view> RdxActivityContext::deserializeState(unsigned state) {
	const Activity::State decodedState = static_cast<Activity::State>(state & kStateMask);
	if (decodedState == Activity::WaitLock) {
		return {decodedState, DescribeMutexMark(static_cast<MutexMark>(state >> kStateShift))};
	} else {
		return {decodedState, ""};
	}
}

unsigned RdxActivityContext::nextId() noexcept {
	static std::atomic<unsigned> idCounter{0u};
	return idCounter.fetch_add(1u, std::memory_order_relaxed);
}

}  // namespace reindexer
