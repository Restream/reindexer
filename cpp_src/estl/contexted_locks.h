#pragma once

#include <chrono>

#include "tools/assertrx.h"

using std::chrono::milliseconds;

namespace reindexer {

constexpr milliseconds kDefaultCondChkTime = milliseconds(50);

struct [[nodiscard]] ignore_cancel_ctx {};

template <typename _Mutex, typename Context>
class [[nodiscard]] contexted_unique_lock {
public:
	using MutexType = _Mutex;

	explicit contexted_unique_lock() noexcept : _M_mtx(nullptr), _M_owns(false), _M_context(nullptr), _M_chkTimeout(kDefaultCondChkTime) {}
	explicit contexted_unique_lock(MutexType& __mtx, Context& __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(false), _M_context(&__context), _M_chkTimeout(__chkTimeout) {
		lock();
	}
	explicit contexted_unique_lock(MutexType& __mtx, std::defer_lock_t, Context& __context,
								   milliseconds __chkTimeout = kDefaultCondChkTime) noexcept
		: _M_mtx(&__mtx), _M_owns(false), _M_context(&__context), _M_chkTimeout(__chkTimeout) {}
	explicit contexted_unique_lock(MutexType& __mtx, std::adopt_lock_t, Context& __context,
								   milliseconds __chkTimeout = kDefaultCondChkTime) noexcept
		: _M_mtx(&__mtx), _M_owns(true), _M_context(&__context), _M_chkTimeout(__chkTimeout) {}
	explicit contexted_unique_lock(MutexType& __mtx, std::try_to_lock_t, Context& __context,
								   milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(__mtx.try_lock()), _M_context(&__context), _M_chkTimeout(__chkTimeout) {}
	contexted_unique_lock(contexted_unique_lock&& lck) noexcept
		: _M_mtx(lck._M_mtx), _M_owns(lck._M_owns), _M_context(lck._M_context), _M_chkTimeout(lck._M_chkTimeout) {
		lck._M_owns = false;
		lck._M_mtx = nullptr;
		lck._M_context = nullptr;
	}
	~contexted_unique_lock() {
		if (_M_owns) {
			_M_mtx->unlock();
		}
	}

	contexted_unique_lock(const contexted_unique_lock&) = delete;
	contexted_unique_lock& operator=(const contexted_unique_lock&) = delete;
	contexted_unique_lock& operator=(contexted_unique_lock&& lck) noexcept {
		if (this != &lck) {
			if (_M_owns) {
				unlock();
			}
			_M_mtx = lck._M_mtx;
			_M_owns = lck._M_owns;
			_M_context = lck._M_context;
			_M_chkTimeout = lck._M_chkTimeout;
			lck._M_owns = false;
			lck._M_mtx = nullptr;
			lck._M_context = nullptr;
		}
		return *this;
	}

	void lock() {
		using namespace std::string_view_literals;
		_M_lockable();
		assertrx(_M_context);
		const auto lockWard = _M_context->BeforeLock(_Mutex::mark);
		if (_M_chkTimeout.count() > 0 && _M_context->IsCancelable()) {
			do {
				ThrowOnCancel(*_M_context, "Write lock (contexted_unique_lock) was canceled or timed out (mutex)"sv);
			} while (!_M_mtx->try_lock_for(_M_chkTimeout));
		} else {
			_M_mtx->lock();
		}
		_M_owns = true;
	}
	void lock(ignore_cancel_ctx) {
		using namespace std::string_view_literals;
		_M_lockable();
		assertrx(_M_context);
		const auto lockWard = _M_context->BeforeLock(_Mutex::mark);
		_M_mtx->lock();
		_M_owns = true;
	}

	bool try_lock() {
		_M_lockable();
		return _M_owns = _M_mtx->try_lock();
	}

	void unlock() {
		assertrx(_M_owns);
		_M_mtx->unlock();
		_M_owns = false;
	}

	MutexType* release() noexcept {
		_M_owns = false;
		auto ret = _M_mtx;
		_M_mtx = nullptr;
		return ret;
	}

	bool owns_lock() const noexcept { return _M_owns; }
	explicit operator bool() const noexcept { return _M_owns; }
	MutexType* mutex() const noexcept { return _M_mtx; }

private:
	void _M_lockable() const noexcept {
		if (_M_mtx == nullptr) {
			assertrx(0);
		}
		if (_M_owns) {
			assertrx(0);
		}
	}

	MutexType* _M_mtx;
	bool _M_owns;
	Context* _M_context;
	milliseconds _M_chkTimeout;
};

template <typename _Mutex, typename Context>
class [[nodiscard]] contexted_shared_lock {
public:
	using MutexType = _Mutex;

	explicit contexted_shared_lock() noexcept : _M_mtx(nullptr), _M_owns(false), _M_context(nullptr), _M_chkTimeout(kDefaultCondChkTime) {}
	explicit contexted_shared_lock(MutexType& __mtx, Context& __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(false), _M_context(&__context), _M_chkTimeout(__chkTimeout) {
		lock();
	}
	explicit contexted_shared_lock(MutexType& __mtx, std::adopt_lock_t, Context& __context,
								   milliseconds __chkTimeout = kDefaultCondChkTime) noexcept
		: _M_mtx(&__mtx), _M_owns(true), _M_context(&__context), _M_chkTimeout(__chkTimeout) {}
	explicit contexted_shared_lock(MutexType& __mtx, std::try_to_lock_t, Context& __context,
								   milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(__mtx.try_lock_shared()), _M_context(&__context), _M_chkTimeout(__chkTimeout) {}
	contexted_shared_lock(contexted_shared_lock&& lck) noexcept
		: _M_mtx(lck._M_mtx), _M_owns(lck._M_owns), _M_context(lck._M_context), _M_chkTimeout(lck._M_chkTimeout) {
		lck._M_owns = false;
		lck._M_mtx = nullptr;
		lck._M_context = nullptr;
	}
	~contexted_shared_lock() {
		if (_M_owns) {
			_M_mtx->unlock_shared();
		}
	}

	contexted_shared_lock(const contexted_shared_lock&) = delete;
	contexted_shared_lock& operator=(const contexted_shared_lock&) = delete;
	contexted_shared_lock& operator=(contexted_shared_lock&& lck) noexcept {
		if (this != &lck) {
			if (_M_owns) {
				unlock();
			}
			_M_mtx = lck._M_mtx;
			_M_owns = lck._M_owns;
			_M_context = lck._M_context;
			_M_chkTimeout = lck._M_chkTimeout;
			lck._M_owns = false;
			lck._M_mtx = nullptr;
			lck._M_context = nullptr;
		}
		return *this;
	}

	void lock() {
		using namespace std::string_view_literals;
		_M_lockable();
		assertrx(_M_context);
		const auto lockWard = _M_context->BeforeLock(_Mutex::mark);
		if (_M_chkTimeout.count() > 0 && _M_context->IsCancelable()) {
			do {
				ThrowOnCancel(*_M_context, "Read lock (contexted_shared_lock) was canceled or timed out (mutex)"sv);
			} while (!_M_mtx->try_lock_shared_for(_M_chkTimeout));
		} else {
			_M_mtx->lock_shared();
		}
		_M_owns = true;
	}

	bool try_lock() {
		_M_lockable();
		return _M_owns = _M_mtx->try_lock_shared();
	}

	void unlock() {
		if (!_M_owns) {
			assertrx(0);
		}
		_M_mtx->unlock_shared();
		_M_owns = false;
	}

	MutexType* release() noexcept {
		_M_owns = false;
		auto ret = _M_mtx;
		_M_mtx = nullptr;
		return ret;
	}

	bool owns_lock() const noexcept { return _M_owns; }
	explicit operator bool() const noexcept { return _M_owns; }
	MutexType* mutex() const noexcept { return _M_mtx; }

private:
	void _M_lockable() const noexcept {
		if (_M_mtx == nullptr) {
			assertrx(0);
		}
		if (_M_owns) {
			assertrx(0);
		}
	}

	MutexType* _M_mtx;
	bool _M_owns;
	Context* _M_context;
	milliseconds _M_chkTimeout;
};

}  // namespace reindexer
