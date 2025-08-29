#include "rpcqrwatcher.h"

namespace reindexer_server {

constexpr static std::chrono::seconds kTimeIncrementPeriod = std::chrono::seconds(1);

constexpr uint32_t kMaxPartsCount = 16;
constexpr uint32_t kMinPartSize = 100;
constexpr uint32_t kFullCheckPeriodSec = 16;
constexpr uint32_t kSingleStepDelay = kFullCheckPeriodSec / kMaxPartsCount;
constexpr double kTimerPeriod = 0.1;

void RPCQrWatcher::Register(net::ev::dynamic_loop& loop, LoggerWrapper logger) {
	timer_.set(loop);
	timer_.set([this, logger = std::move(logger)](net::ev::timer&, int) {
		thread_local static auto lastCbCallTime = steady_clock_w::now_coarse();
		constexpr auto secCount = std::chrono::duration_cast<std::chrono::seconds>(kTimeIncrementPeriod).count();
		static_assert(secCount >= 1, "Minimal timer granularity is 1 second");

		const auto realTime = steady_clock_w::now_coarse();
		if (realTime - lastCbCallTime < kTimeIncrementPeriod) {
			return;
		}
		lastCbCallTime = realTime;

		const auto now = nowSeconds_.fetch_add(secCount, std::memory_order_relaxed) + secCount;
		static_assert(kSingleStepDelay > 0, "Each step must have some delay");

		if (now % 600 == 0) {
			logger.info("Allocated qrs: {} ", allocated_.load(std::memory_order_relaxed));
		}

		if (idleTimeout_.count() > 0 && now % kSingleStepDelay == 0) {
			const uint32_t size = allocated_.load(std::memory_order_acquire);
			uint32_t from = 0;
			uint32_t to = size;
			thread_local static uint32_t prevToValue = std::numeric_limits<uint32_t>::max();
			if (size > kMinPartSize) {
				const uint32_t expectedPartsCount = std::min(size / kMinPartSize, kMaxPartsCount);
				const uint32_t partSize = size / expectedPartsCount + 1;
				if (prevToValue != std::numeric_limits<uint32_t>::max()) {
					from = prevToValue;
				}
				to = std::min(from + partSize, size);
			}
			prevToValue = (to == size) ? 0 : to;
			const auto bt = steady_clock_w::now_coarse();
			const auto cnt = removeExpired(now, from, to);
			if (cnt) {
				const auto et = steady_clock_w::now_coarse();
				logger.info("{} query results were removed due to idle timeout. Cleanup time: {} us", cnt,
							std::chrono::duration_cast<std::chrono::microseconds>(et - bt).count());
			}
		}
	});
	timer_.start(kTimerPeriod, kTimerPeriod);
}

void RPCQrWatcher::Stop() {
	timer_.stop();
	timer_.reset();
}

uint32_t RPCQrWatcher::removeExpired(uint32_t now, uint32_t from, uint32_t to) {
	uint32_t expiredCnt = 0;
	[[maybe_unused]] const auto allocated = allocated_.load(std::memory_order_acquire);
	assertf(to <= allocated, "to: {}, allocated: {}", to, allocated);
	for (uint32_t i = from; i < to; ++i) {
		auto& qrs = qrs_[i];
		if (qrs.IsExpired(now, idleTimeout_.count())) {
			UID curUID = qrs.uid.load(std::memory_order_acquire);
			UID newUID;
			bool shouldClearQRs;
			do {
				shouldClearQRs = qrs.IsExpired(curUID, now, idleTimeout_.count());
				if (shouldClearQRs) {
					newUID = curUID;
					newUID.state = UID::ClearingInProgress;
				} else {
					break;
				}
			} while (!qrs.uid.compare_exchange_strong(curUID, newUID, std::memory_order_acq_rel));
			if (shouldClearQRs) {
				qrs.qr = QueryResults();
				newUID.SetUnitialized();
				qrs.uid.store(newUID, std::memory_order_release);
				++expiredCnt;

				lock_guard lck(mtx_);
				putFreeID(i);
			}
		}
	}
	return expiredCnt;
}

}  // namespace reindexer_server
