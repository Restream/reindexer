#pragma once

#include <chrono>
#include <functional>
#include <mutex>

#include "tools/errors.h"

using std::chrono::milliseconds;
using std::try_to_lock_t;
using std::defer_lock_t;
using std::adopt_lock_t;

namespace reindexer {

const milliseconds kDefaultCondChkTime = milliseconds(20);

template <typename _Mutex, typename Context>
class contexted_lock_guard {
public:
	using MutexType = _Mutex;

	explicit contexted_lock_guard(MutexType& __mtx, Context& __context, milliseconds __chkTimeout = kDefaultCondChkTime) : _M_mtx(__mtx) {
		const auto lockWard = __context.BeforeLock(_Mutex::mark);
		if (__chkTimeout.count() > 0 && __context.isCancelable()) {
			do {
				ThrowOnCancel(__context, "Lock was canceled on condition");
			} while (!_M_mtx.try_lock_for(__chkTimeout));
		} else {
			_M_mtx.lock();
		}
	}
	~contexted_lock_guard() { _M_mtx.unlock(); }

private:
	MutexType& _M_mtx;
};

template <typename _Mutex, typename Context>
class contexted_unique_lock {
public:
	using MutexType = _Mutex;

	explicit contexted_unique_lock(MutexType& __mtx, Context* __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(false), _M_context(__context), _M_chkTimeout(__chkTimeout) {
		assert(_M_context);
		lock();
	}
	explicit contexted_unique_lock(MutexType& __mtx, defer_lock_t, Context* __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(false), _M_context(__context), _M_chkTimeout(__chkTimeout) {
		assert(_M_context);
	}
	explicit contexted_unique_lock(MutexType& __mtx, adopt_lock_t, Context* __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(true), _M_context(__context), _M_chkTimeout(__chkTimeout) {
		assert(_M_context);
	}
	explicit contexted_unique_lock(MutexType& __mtx, try_to_lock_t, Context* __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(__mtx.try_lock()), _M_context(__context), _M_chkTimeout(__chkTimeout) {
		assert(_M_context);
	}
	~contexted_unique_lock() {
		if (_M_owns) _M_mtx->unlock();
	}

	contexted_unique_lock(const contexted_unique_lock&) = delete;
	contexted_unique_lock& operator=(const contexted_unique_lock&) = delete;

	contexted_unique_lock(contexted_unique_lock&& __sl) noexcept : contexted_unique_lock() { swap(__sl); }

	contexted_unique_lock& operator=(contexted_unique_lock&& __sl) noexcept {
		contexted_unique_lock(std::move(__sl)).swap(*this);
		return *this;
	}

	void lock() {
		_M_lockable();
		assert(_M_context);
		const auto lockWard = _M_context->BeforeLock(_Mutex::mark);
		if (_M_chkTimeout.count() > 0 && _M_context->isCancelable()) {
			do {
				ThrowOnCancel(*_M_context, "Lock was canceled on condition");
			} while (!_M_mtx->try_lock_for(_M_chkTimeout));
		} else {
			_M_mtx->lock();
		}
		_M_owns = true;
	}

	bool try_lock() {
		_M_lockable();
		return _M_owns = _M_mtx->try_lock();
	}

	void unlock() {
		if (!_M_owns) assert(0);
		_M_mtx->unlock();
		_M_owns = false;
	}

	void swap(contexted_unique_lock& __u) noexcept {
		std::swap(_M_mtx, __u._M_mtx);
		std::swap(_M_owns, __u._M_owns);
		std::swap(_M_context, __u._M_context);
		std::swap(_M_chkTimeout, __u._M_chkTimeout);
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
	void _M_lockable() const {
		if (_M_mtx == nullptr) assert(0);
		if (_M_owns) assert(0);
	}

	MutexType* _M_mtx;
	bool _M_owns;
	Context* _M_context;
	milliseconds _M_chkTimeout;
};

/// Swap specialization for contexted_unique_lock
template <typename _Mutex, typename Context1, typename Context2>
void swap(contexted_unique_lock<_Mutex, Context1>& __x, contexted_unique_lock<_Mutex, Context2>& __y) noexcept {
	__x.swap(__y);
}

template <typename _Mutex, typename Context>
class contexted_shared_lock {
public:
	using MutexType = _Mutex;

	explicit contexted_shared_lock(MutexType& __mtx, Context* __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(false), _M_context(__context), _M_chkTimeout(__chkTimeout) {
		assert(_M_context);
		lock();
	}
	~contexted_shared_lock() {
		if (_M_owns) _M_mtx->unlock_shared();
	}

	contexted_shared_lock(const contexted_shared_lock&) = delete;
	contexted_shared_lock& operator=(const contexted_shared_lock&) = delete;

	contexted_shared_lock(contexted_shared_lock&& __sl) noexcept : contexted_shared_lock() { swap(__sl); }

	contexted_shared_lock& operator=(contexted_shared_lock&& __sl) noexcept {
		contexted_shared_lock(std::move(__sl)).swap(*this);
		return *this;
	}

	void lock() {
		_M_lockable();
		assert(_M_context);
		const auto lockWard = _M_context->BeforeLock(_Mutex::mark);
		if (_M_chkTimeout.count() > 0 && _M_context->isCancelable()) {
			do {
				ThrowOnCancel(*_M_context, "Lock was canceled on condition");
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
		if (!_M_owns) assert(0);
		_M_mtx->unlock_shared();
		_M_owns = false;
	}

	void swap(contexted_shared_lock& __u) noexcept {
		std::swap(_M_mtx, __u._M_mtx);
		std::swap(_M_owns, __u._M_owns);
		std::swap(_M_context, __u._M_context);
		std::swap(_M_chkTimeout, __u._M_chkTimeout);
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
	void _M_lockable() const {
		if (_M_mtx == nullptr) assert(0);
		if (_M_owns) assert(0);
	}

	MutexType* _M_mtx;
	bool _M_owns;
	Context* _M_context;
	milliseconds _M_chkTimeout;
};

/// Swap specialization for contexted_shared_lock
template <typename _Mutex, typename Context1, typename Context2>
void swap(contexted_shared_lock<_Mutex, Context1>& __x, contexted_shared_lock<_Mutex, Context2>& __y) noexcept {
	__x.swap(__y);
}

}  // namespace reindexer
