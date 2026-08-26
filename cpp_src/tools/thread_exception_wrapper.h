#pragma once

#include <exception>
#include "estl/lock.h"
#include "estl/mutex.h"

namespace reindexer {

class [[nodiscard]] ExceptionPtrWrapper {
public:
	void SetException(std::exception_ptr&& ptr) noexcept {
		lock_guard lck(mtx_);
		if (!ex_) {
			ex_ = std::move(ptr);
			hasException_.store(true, std::memory_order_relaxed);
		}
	}
	void RethrowException() {
		lock_guard lck(mtx_);
		if (ex_) {
			auto ptr = std::move(ex_);
			ex_ = nullptr;
			std::rethrow_exception(std::move(ptr));
		}
	}
	bool HasException() const noexcept { return hasException_.load(std::memory_order_relaxed); }

private:
	std::atomic<bool> hasException_ = false;
	std::exception_ptr ex_ = nullptr;
	mutable mutex mtx_;
};

}  // namespace reindexer
