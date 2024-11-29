#include "activity_context.h"
#include <iomanip>
#include <sstream>
#include "activity_context.h"
#include "cjson/jsonbuilder.h"
#include "tools/stringstools.h"

namespace reindexer {

using namespace std::string_view_literals;

void ActivityContainer::Register(const RdxActivityContext* context) {
	std::unique_lock lck(mtx_);
	const auto res = cont_.insert(context);
	lck.unlock();

	assertrx(res.second);
	(void)res;
}

void ActivityContainer::Unregister(const RdxActivityContext* context) {
	std::unique_lock lck(mtx_);
	const auto count = cont_.erase(context);
	lck.unlock();

	assertrx(count == 1u);
	(void)count;
}

void ActivityContainer::Reregister(const RdxActivityContext* oldCtx, const RdxActivityContext* newCtx) {
	if (oldCtx == newCtx) {
		return;
	}

	std::unique_lock lck(mtx_);
	const auto eraseCount = cont_.erase(oldCtx);
	const auto insertRes = cont_.insert(newCtx);
	lck.unlock();

	assertrx(eraseCount == 1u);
	assertrx(insertRes.second);
	(void)eraseCount;
	(void)insertRes;
}

std::vector<Activity> ActivityContainer::List() {
	std::vector<Activity> ret;
	{
		std::lock_guard lck(mtx_);
		ret.reserve(cont_.size());
		for (const RdxActivityContext* ctx : cont_) {
			ret.emplace_back(*ctx);
		}
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

std::string_view Activity::DescribeState(State st) noexcept {
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
	if (!user.empty()) {
		builder.Put("user", user);
	}
	builder.Put("query", query);
	builder.Put("query_id", id);
	std::time_t t = system_clock_w::to_time_t(startTime);
	char buffer[80];
	std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
	std::stringstream ss;
	ss << buffer << '.' << std::setw(3) << std::setfill('0') << (duration_cast<milliseconds>(startTime.time_since_epoch()).count() % 1000);
	builder.Put("query_start", ss.str());
	builder.Put("state", DescribeState(state));
	if (state == WaitLock) {
		builder.Put("lock_description", "Wait lock for " + std::string(description));
	}
	builder.End();
}

RdxActivityContext::RdxActivityContext(std::string_view activityTracer, std::string_view user, std::string_view query,
									   ActivityContainer& parent, int ipConnectionId, bool clientState)
	: data_{nextId(),		std::string(activityTracer), std::string(user),		std::string(query),
			ipConnectionId, Activity::InProgress,		 system_clock_w::now(), ""sv},
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

}  // namespace reindexer
