#pragma once

#include <atomic>
#include <thread>
#include "thread_annotation_attributes.h"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] spinlock {
public:
	spinlock() { _M_lock.clear(); }
	spinlock(const spinlock&) = delete;
	~spinlock() = default;

	void lock() noexcept {
		for (unsigned int i = 1; !try_lock(); ++i) {
			if ((i & 0xff) == 0) {
				std::this_thread::yield();
			}
		}
	}
	bool try_lock() noexcept { return !_M_lock.test_and_set(std::memory_order_acq_rel); }
	void unlock() noexcept { _M_lock.clear(std::memory_order_release); }

	const spinlock& operator!() const& { return *this; }
	auto operator!() const&& = delete;

private:
	std::atomic_flag _M_lock;
};

class [[nodiscard]] RX_CAPABILITY("mutex") read_write_spinlock {
public:
	read_write_spinlock() = default;
	~read_write_spinlock() = default;
	read_write_spinlock(const read_write_spinlock&) = delete;

	void lock_shared() noexcept {
		for (;;) {
			int32_t currLock = lock_.load(std::memory_order_acquire);
			while ((currLock & 0x80000000) != 0) {
				std::this_thread::yield();
				currLock = lock_.load(std::memory_order_acquire);
			}

			int32_t oldLock = (currLock & 0x7fffffff);
			int32_t newLock = oldLock + 1;

			if (lock_.compare_exchange_strong(oldLock, newLock, std::memory_order_acq_rel)) {
				return;
			}
		}
	}

	void unlock_shared() noexcept { --lock_; }

	void lock() noexcept {
		for (;;) {
			int32_t oldLock = (lock_.load(std::memory_order_acquire) & 0x7fffffff);
			int32_t newLock = (oldLock | 0x80000000);
			if (lock_.compare_exchange_strong(oldLock, newLock, std::memory_order_acq_rel)) {
				while ((lock_.load(std::memory_order_acquire) & 0x7fffffff) != 0) {
					std::this_thread::yield();
				}
				return;
			}
		}
	}

	void unlock() noexcept { lock_ = 0; }

	const read_write_spinlock& operator!() const& { return *this; }
	auto operator!() const&& = delete;

private:
	std::atomic<int32_t> lock_ = {0};
};

class [[nodiscard]] RX_CAPABILITY("mutex") read_write_recursive_spinlock : public read_write_spinlock {
public:
	read_write_recursive_spinlock() = default;
	~read_write_recursive_spinlock() = default;
	read_write_recursive_spinlock(const read_write_recursive_spinlock&) = delete;

	void lock() noexcept {
		std::thread::id currThreadID = threadID_.load(std::memory_order_acquire);
		if (currThreadID != std::this_thread::get_id()) {
			read_write_spinlock::lock();
			assertrx(recursiveDepth_ == 0);
			threadID_.store(currThreadID, std::memory_order_release);
		}
		recursiveDepth_++;
	}

	void unlock() noexcept {
		assertrx(recursiveDepth_ > 0);
		if (--recursiveDepth_ == 0) {
			threadID_ = std::thread::id{};
			read_write_spinlock::unlock();
		}
	}

	const read_write_recursive_spinlock& operator!() const& { return *this; }
	auto operator!() const&& = delete;

private:
	std::atomic<std::thread::id> threadID_{std::thread::id{}};
	int32_t recursiveDepth_{0};
};

}  // namespace reindexer
