#include "networkmonitor.h"
#include "sharding.h"

namespace reindexer {
namespace sharding {

constexpr auto kRetryInterval = std::chrono::seconds(1);

void NetworkMonitor::Configure(ConnectionsMap& hostsConnections, std::chrono::seconds defaultTimeout,
							   std::chrono::milliseconds statusCallTimeout) {
	std::lock_guard lck(mtx_);
	succeed_.clear();
	hostsConnections_ = &hostsConnections;
	connectionsTotal_ = 0;
	defaultTimeout_ = defaultTimeout;
	statusCallTimeout_ = statusCallTimeout;
	for (auto& connListP : (*hostsConnections_)) {
		connectionsTotal_ += connListP.second.size();
	}
}

Error NetworkMonitor::AwaitShards(const InternalRdxContext& ctx) noexcept {
	Error err;
	try {
		InternalRdxContext defaultCtx;

		std::unique_lock lck(mtx_);
		if (!defaultTimeout_.count()) {
			return Error();
		}
		if (!hostsConnections_) {
			return Error(errLogic, "Shards' host connections are not configured");
		}
		const InternalRdxContext* actualCtx = &ctx;
		if (!actualCtx->HasDeadline() && defaultTimeout_.count() > 0) {
			defaultCtx.SetTimeout(defaultTimeout_);
			actualCtx = &defaultCtx;
		}
		do {
			if (!err.ok()) {
				lck.unlock();
				ThrowOnCancel(*actualCtx->DeadlineCtx());
				std::this_thread::sleep_for(kRetryInterval);
				ThrowOnCancel(*actualCtx->DeadlineCtx());
				lck.lock();
				if (!hostsConnections_) {
					return Error(errLogic, "Shards' host connections were reset");
				}
			}
			if (!terminated_) {
				sendStatusRequests();
				err = awaitStatuses(lck, *actualCtx);
			}
		} while (!err.ok() && !terminated_);
		if (terminated_) {
			return Error(errTerminated, "Sharding proxy was shutdowned");
		}
	} catch (Error& err) {
		if (err.code() == errTimeout || err.code() == errCanceled) {
			return Error(err.code(), "Some of the shards are not available (request was canceled/timed out)");
		}
		return err;
	} catch (...) {
		return Error(errLogic, "Unknow error in sharding network monitor");
	}
	return err;
}

void NetworkMonitor::Shutdown() {
	std::unique_lock lck(mtx_);
	terminated_ = true;
	lck.unlock();
	cv_.notify_all();
}

void NetworkMonitor::sendStatusRequests() {
	if (inProgress_) return;

	inProgress_ = true;
	executed_ = 0;
	succeed_.clear();
	for (auto& connListP : (*hostsConnections_)) {
		for (auto& conn : connListP.second) {
			auto err = conn->WithTimeout(statusCallTimeout_)
						   .WithCompletion([this, idx = connListP.first](const Error& e) {
							   std::unique_lock lck(mtx_);
							   ++executed_;
							   if (e.ok()) {
								   succeed_.emplace(idx);
							   }
							   if (isStatusesReady()) {
								   lck.unlock();
								   cv_.notify_all();
							   }
						   })
						   .Status();
			if (!err.ok()) {
				++executed_;
			}
		}
	}
}

Error NetworkMonitor::awaitStatuses(std::unique_lock<std::mutex>& lck, const InternalRdxContext& ctx) {
	if (inProgress_) {
		assert(ctx.DeadlineCtx());
		cv_.wait(
			lck, [this] { return isStatusesReady(); }, *ctx.DeadlineCtx());
		inProgress_ = false;
	}
	if (hostsConnections_->size() == succeed_.size()) {
		return Error();
	}

	return Error(errTimeout, "Shards are not connected yet");
}

bool NetworkMonitor::isStatusesReady() const noexcept {
	return hostsConnections_->size() == succeed_.size() || connectionsTotal_ == executed_;
}

}  // namespace sharding
}  // namespace reindexer
