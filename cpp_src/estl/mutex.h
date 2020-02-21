#pragma once

#include <atomic>

namespace reindexer {

class string_view;

class dummy_mutex {
public:
	void lock() {}
	void unlock() {}
};

enum class MutexMark : unsigned { DbManager = 1u, IndexText, Namespace, Reindexer, ReindexerStorage };
string_view DescribeMutexMark(MutexMark);

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

	void lock() {
		while (_M_lock.test_and_set(std::memory_order_acq_rel))
			;
	}
	bool try_lock() { return !_M_lock.test_and_set(std::memory_order_acq_rel); }
	void unlock() { _M_lock.clear(std::memory_order_release); }

private:
	std::atomic_flag _M_lock;
};

}  // namespace reindexer
