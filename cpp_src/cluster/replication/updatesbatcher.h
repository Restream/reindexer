#pragma once

#include "replicationthread.h"

namespace reindexer {
namespace cluster {

constexpr size_t kAsyncUpdatesRoutineStackSize = 64 * 1024;

template <typename UpdateT, typename ContextT, typename ApplyUpdateFnT, typename OnUpdateResultFnT, typename ConvertResultFnT>
class [[nodiscard]] UpdatesBatcher {
public:
	UpdatesBatcher(net::ev::dynamic_loop& loop, size_t coroCount, ApplyUpdateFnT&& applyUpdateF, OnUpdateResultFnT&& onUpdateResult,
				   ConvertResultFnT&& convert)
		: applyUpdateF_(std::move(applyUpdateF)), onUpdateResult_(std::move(onUpdateResult)), convert_(std::move(convert)) {
		channels_.reserve(coroCount);
		for (size_t i = 0; i < coroCount; ++i) {
			channels_.emplace_back(std::make_unique<ResultChT>());
			loop.spawn(
				workersWg_,
				[this]() noexcept {
					while (true) {
						auto itp = updatesCh_.pop();
						if (!itp.second) {
							break;
						}
						auto update = itp.first;
						auto err = applyUpdateF_(*update.upd, update.ctx).err;
						getResultCh(update.id).push(std::make_pair(std::move(update), std::move(err)));
					}
				},
				kAsyncUpdatesRoutineStackSize);
		}
	}
	~UpdatesBatcher() {
		assert(!batchedUpdatesCount_);
		updatesCh_.close();
		workersWg_.wait();
	}

	UpdateApplyStatus Batch(const UpdateT& upd, ContextT&& ctx) {
		UpdateApplyStatus res;

		if (batchedUpdatesCount_) {	 // while?
			while (!channels_[nextResultsCh_]->empty() || updatesCh_.full()) {
				--batchedUpdatesCount_;
				res = awaitNextResult();
				if (!res.err.ok()) {
					return res;
				}
			}
		}

		updatesCh_.push(ContextedUpdate{&upd, std::move(ctx), nextId_++});
		++batchedUpdatesCount_;
		return res;
	}

	UpdateApplyStatus AwaitBatchedUpdates() {
		UpdateApplyStatus aggregatedResult;
		while (batchedUpdatesCount_) {
			--batchedUpdatesCount_;
			auto res = awaitNextResult();
			if (!res.err.ok() && aggregatedResult.err.ok()) {
				aggregatedResult = std::move(res);
			}
		}
		return aggregatedResult;
	}
	size_t BatchedUpdatesCount() const noexcept { return batchedUpdatesCount_; }

private:
	UpdateApplyStatus awaitNextResult() {
		auto& results = *(channels_[nextResultsCh_]);
		nextResultsCh_ = (nextResultsCh_ + 1) % channels_.size();
		auto res = results.pop().first;
		auto& update = res.first;
		auto err = convert_(std::move(res.second), *update.upd);
		onUpdateResult_(*update.upd, err, std::move(update.ctx));
		return err;
	}

	struct [[nodiscard]] ContextedUpdate {
		const UpdateT* upd;
		ContextT ctx;
		uint64_t id;
	};
	using ResultChT = coroutine::channel<std::pair<ContextedUpdate, Error>>;

	ResultChT& getResultCh(uint64_t id) noexcept { return *(channels_[id % channels_.size()]); }

	size_t batchedUpdatesCount_ = 0;
	coroutine::channel<ContextedUpdate> updatesCh_;
	coroutine::wait_group workersWg_;
	ApplyUpdateFnT applyUpdateF_;
	OnUpdateResultFnT onUpdateResult_;
	ConvertResultFnT convert_;
	std::vector<std::unique_ptr<ResultChT>> channels_;
	uint32_t nextResultsCh_ = 0;
	uint64_t nextId_ = 0;
};

}  // namespace cluster
}  // namespace reindexer
