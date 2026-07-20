#include "networkmonitor.h"
#include <thread>
#include "estl/lock.h"
#include "estl/mutex.h"
#include "sharding.h"

namespace reindexer {
namespace sharding {

constexpr auto kRetryInterval = std::chrono::seconds(1);

void NetworkMonitor::Configure(ConnectionsMap& hostsConnections, std::chrono::seconds defaultTimeout,
							   std::chrono::milliseconds statusCallTimeout) {
	lock_guard lck(mtx_);
	succeed_.clear();
	hostsConnections_ = &hostsConnections;
	connectionsTotal_ = 0;
	defaultTimeout_ = defaultTimeout;
	statusCallTimeout_ = statusCallTimeout;
	for (auto& connListP : (*hostsConnections_)) {
		connectionsTotal_ += connListP.second.size();
	}
}

Error NetworkMonitor::AwaitShards(const RdxContext& ctx) noexcept {
	Error err;
	try {
		const RdxDeadlineContext deadlineCtx(defaultTimeout_, ctx.GetCancelCtx());
		std::unique_ptr<const RdxContext> defaultCtx;

		unique_lock lck(mtx_);
		if (defaultTimeout_.count() <= 0) {
			return Error();
		}
		if (!hostsConnections_) {
			return Error(errLogic, "Shards' host connections are not configured");
		}
		const RdxContext* actualCtx = &ctx;
		if (!actualCtx->IsCancelable()) {
			defaultCtx = std::make_unique<RdxContext>(ctx.WithCancelCtx(deadlineCtx));
			actualCtx = defaultCtx.get();
		}
		do {
			if (!err.ok()) {
				lck.unlock();
				ThrowOnCancel(*actualCtx);
				std::this_thread::sleep_for(kRetryInterval);
				ThrowOnCancel(*actualCtx);
				lck.lock();
				if (!hostsConnections_) {
					return Error(errLogic, "Shards' host connections were reset");
				}
			}
			if (!terminated_) {
				sendStatusRequests();
				err = awaitStatuses(lck, *actualCtx);
			}
		} while (!err.ok() && err.code() == errTimeout && !terminated_);
		if (terminated_) {
			return Error(errTerminated, "Sharding proxy has been shutdown");
		}
	} catch (Error& err) {
		if (err.code() == errTimeout || err.code() == errCanceled) {
			return Error(err.code(), "Some of the shards are not available (request was canceled/timed out)");
		}
		return err;
	} catch (...) {
		return Error(errLogic, "Unknown error in sharding network monitor");
	}
	return err;
}

void NetworkMonitor::Shutdown() {
	unique_lock lck(mtx_);
	terminated_ = true;
	lck.unlock();
	cv_.notify_all();
}

void NetworkMonitor::sendStatusRequests() {
	if (inProgress_) {
		return;
	}

	inProgress_ = true;
	executed_ = 0;
	succeed_.clear();
	for (auto& connListP : (*hostsConnections_)) {
		for (auto& conn : connListP.second) {
			auto err = conn->WithTimeout(statusCallTimeout_)
						   .WithCompletion([this, idx = connListP.first](const Error& e) {
							   unique_lock lck(mtx_);
							   ++executed_;
							   if (e.ok()) {
								   succeed_.emplace(idx);
							   }
							   lastCompletionError_ = e;
							   if (areStatusesReady()) {
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

Error NetworkMonitor::awaitStatuses(unique_lock<recursive_mutex>& lck, const RdxContext& ctx) {
	if (inProgress_) {
		assertrx(ctx.IsCancelable());
		cv_.wait(lck, [this] { return areStatusesReady(); }, ctx);
		inProgress_ = false;
	}
	if (hostsConnections_->size() == succeed_.size()) {
		return Error();
	}
	if (lastCompletionError_.code() == errConnectSSL) {
		return lastCompletionError_;
	} else {
		return Error(errTimeout, "Shards are not connected yet");
	}
}

bool NetworkMonitor::areStatusesReady() const noexcept {
	return hostsConnections_->size() == succeed_.size() || connectionsTotal_ == executed_;
}

}  // namespace sharding
}  // namespace reindexer
