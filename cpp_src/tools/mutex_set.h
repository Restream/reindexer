#pragma once

#include <array>
#include "estl/contexted_locks.h"
#include "estl/marked_mutex.h"
#include "estl/timed_mutex.h"
#include "tools/stringstools.h"

namespace reindexer {

template <typename CtxT, MutexMark mark, size_t size>
class [[nodiscard]] MutexSet {
	using MutexT = MarkedMutex<timed_mutex, mark>;
	using UniqueLockT = contexted_unique_lock<MutexT, const CtxT>;
	using HasherT = nocase_hash_str;

public:
	MutexSet() {
		for (auto& mtxPtr : mtxs_) {
			mtxPtr = std::make_unique<MutexT>();
		}
	}

	class [[nodiscard]] Locks {
	public:
		Locks() = default;
		Locks(MutexT& mtx, const CtxT& ctx) { lck1_ = UniqueLockT(mtx, ctx); }
		Locks(MutexT& mtx1, MutexT& mtx2, const CtxT& ctx) {
			if (&mtx1 == &mtx2) {
				lck1_ = UniqueLockT(mtx1, ctx);
			} else {
				lck1_ = UniqueLockT(*(std::min(&mtx1, &mtx2)), ctx);
				lck2_ = UniqueLockT(*(std::max(&mtx1, &mtx2)), ctx);
			}
		}
		Locks(Locks&&) noexcept = default;
		Locks& operator=(Locks&&) noexcept = default;
		void UnlockIfOwns() noexcept { unlock(); }
		// std-compatible naming
		void lock() {
			if (lck1_.mutex()) {
				lck1_.lock();
			}
			if (lck2_.mutex()) {
				lck2_.lock();
			}
		}
		// std-compatible naming
		void unlock() {
			if (lck1_.owns_lock()) {
				lck1_.unlock();
			}
			if (lck2_.owns_lock()) {
				lck2_.unlock();
			}
		}

	private:
		UniqueLockT lck1_;
		UniqueLockT lck2_;
	};

	Locks Lock(std::string_view key, const CtxT& ctx) {
		static_assert(size > 0, "Size can not be 0");
		const auto idx = HasherT()(key) % size;
		return {*mtxs_[idx], ctx};
	}
	Locks Lock(std::string_view key1, std::string_view key2, const CtxT& ctx) {
		static_assert(size > 0, "Size can not be 0");
		const auto idx1 = HasherT()(key1) % size;
		const auto idx2 = HasherT()(key2) % size;
		return {*mtxs_[idx1], *mtxs_[idx2], ctx};
	}

private:
	std::array<std::unique_ptr<MutexT>, size> mtxs_;
};

}  // namespace reindexer
