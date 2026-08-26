#pragma once

#include "estl/lock.h"
#include "estl/thread_annotation_attributes.h"

namespace reindexer {

template <typename Mtx1, typename Mtx2>
class [[nodiscard]] RX_SCOPED_CAPABILITY DoubleUniqueLock : private unique_lock<Mtx1>, unique_lock<Mtx2> {
	static_assert(!std::is_same_v<Mtx1, Mtx2>);

public:
	explicit DoubleUniqueLock(Mtx1& mtx1, Mtx2& mtx2) RX_ACQUIRE(mtx1, mtx2) : unique_lock<Mtx1>(mtx1), unique_lock<Mtx2>(mtx2) {}
	~DoubleUniqueLock() RX_RELEASE() = default;
	void unlock() RX_RELEASE() {
		unique_lock<Mtx2>::unlock();
		unique_lock<Mtx1>::unlock();
	}
	bool OwnsThis(const Mtx1& mtx) const noexcept { return unique_lock<Mtx1>::owns_lock() && unique_lock<Mtx1>::mutex() == &mtx; }
	bool OwnsThis(const Mtx2& mtx) const noexcept { return unique_lock<Mtx2>::owns_lock() && unique_lock<Mtx2>::mutex() == &mtx; }
	constexpr static std::enable_if_t<Mtx1::mark == Mtx2::mark, decltype(Mtx1::mark)> kMutexMark = Mtx1::mark;
};

}  // namespace reindexer
