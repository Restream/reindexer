#pragma once

#include <atomic>
#include <string_view>
#include <thread>
#include "tools/assertrx.h"

namespace reindexer {

class dummy_mutex {
public:
	void lock() {}
	void unlock() {}
	void lock_shared() {}
	void unlock_shared() {}
};

enum class MutexMark : unsigned { DbManager = 1u, IndexText, Namespace, Reindexer, ReindexerStorage };
std::string_view DescribeMutexMark(MutexMark);

template <typename Mutex, MutexMark m>
class MarkedMutex : public Mutex {
public:
	constexpr static MutexMark mark = m;
};

class spinlock {
public:
	spinlock() { _M_lock.clear(); }
	spinlock(const spinlock&) = delete;
	~spinlock() = default;

	void lock() noexcept {
		for (unsigned int i = 1; !try_lock(); ++i)
			if ((i & 0xff) == 0) std::this_thread::yield();
	}
	bool try_lock() noexcept { return !_M_lock.test_and_set(std::memory_order_acq_rel); }
	void unlock() noexcept { _M_lock.clear(std::memory_order_release); }

private:
	std::atomic_flag _M_lock;
};

class read_write_spinlock {
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

private:
	std::atomic<int32_t> lock_ = {0};
};

class read_write_recursive_spinlock : public read_write_spinlock {
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

private:
	std::atomic<std::thread::id> threadID_{std::thread::id{}};
	int32_t recursiveDepth_{0};
};
}  // namespace reindexer
