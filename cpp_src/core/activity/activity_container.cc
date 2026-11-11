#include "activity_container.h"
#include "activity_context.h"
#include "estl/lock.h"
#include "tools/stringstools.h"

namespace reindexer {

void ActivityContainer::Register(const RdxActivityContext* context) {
	unique_lock lck(mtx_);
	const auto res = cont_.insert(context);
	lck.unlock();

	assertrx(res.second);
	(void)res;
#ifdef RX_LOGACTIVITY
	log_.Register(context);
#endif
}

void ActivityContainer::Unregister(const RdxActivityContext* context) {
	unique_lock lck(mtx_);
	const auto count = cont_.erase(context);
	lck.unlock();

	assertrx(count == 1u);
	(void)count;
#ifdef RX_LOGACTIVITY
	log_.Unregister(context);
#endif
}

void ActivityContainer::Reregister(const RdxActivityContext* oldCtx, const RdxActivityContext* newCtx) {
	if (oldCtx == newCtx) {
		return;
	}

	unique_lock lck(mtx_);
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
	lock_guard lck(mtx_);
	log_.Reset();
#endif
}

#ifdef RX_LOGACTIVITY
void ActivityContainer::AddOperation(const RdxActivityContext* ctx, Activity::State st, bool start) {
	unique_lock lck(mtx_);
	log_.AddOperation(ctx, st, start);
}
#endif

std::vector<Activity> ActivityContainer::List([[maybe_unused]] int serverId) {
	std::vector<Activity> ret;
	{
		lock_guard lck(mtx_);

#ifdef RX_LOGACTIVITY
		log_.Dump(serverId);
#endif

		ret.reserve(cont_.size());
		for (const RdxActivityContext* ctx : cont_) {
			ret.emplace_back(*ctx);
		}
	}
	return ret;
}

std::optional<std::string> ActivityContainer::QueryForIpConnection(int id) {
	lock_guard lck(mtx_);

	for (const RdxActivityContext* ctx : cont_) {
		if (ctx->CheckConnectionId(id)) {
			std::string ret;
			deepCopy(ret, ctx->Query());
			return std::optional{std::move(ret)};
		}
	}

	return std::nullopt;
}

}  // namespace reindexer
