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
class contexted_unique_lock {
public:
	using MutexType = _Mutex;

	explicit contexted_unique_lock() : _M_mtx(nullptr), _M_owns(false), _M_context(nullptr), _M_chkTimeout(kDefaultCondChkTime) {}
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
	contexted_unique_lock(contexted_unique_lock&& lck)
		: _M_mtx(lck._M_mtx), _M_owns(lck._M_owns), _M_context(lck._M_context), _M_chkTimeout(lck._M_chkTimeout) {
		lck._M_owns = false;
		lck._M_mtx = nullptr;
		lck._M_context = nullptr;
	}
	~contexted_unique_lock() {
		if (_M_owns) _M_mtx->unlock();
	}

	contexted_unique_lock(const contexted_unique_lock&) = delete;
	contexted_unique_lock& operator=(const contexted_unique_lock&) = delete;
	contexted_unique_lock& operator=(contexted_unique_lock&& lck) {
		if (this != &lck) {
			if (_M_owns) unlock();
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
		_M_lockable();
		assert(_M_context);
		const auto lockWard = _M_context->BeforeLock(_Mutex::mark);
		if (_M_chkTimeout.count() > 0 && _M_context->isCancelable()) {
			do {
				ThrowOnCancel(*_M_context, "Lock was canceled on condition"_sv);
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

template <typename _Mutex, typename Context>
class contexted_shared_lock {
public:
	using MutexType = _Mutex;

	explicit contexted_shared_lock() : _M_mtx(nullptr), _M_owns(false), _M_context(nullptr), _M_chkTimeout(kDefaultCondChkTime) {}
	explicit contexted_shared_lock(MutexType& __mtx, Context* __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(false), _M_context(__context), _M_chkTimeout(__chkTimeout) {
		assert(_M_context);
		lock();
	}
	explicit contexted_shared_lock(MutexType& __mtx, adopt_lock_t, Context* __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(true), _M_context(__context), _M_chkTimeout(__chkTimeout) {
		assert(_M_context);
	}
	explicit contexted_shared_lock(MutexType& __mtx, try_to_lock_t, Context* __context, milliseconds __chkTimeout = kDefaultCondChkTime)
		: _M_mtx(&__mtx), _M_owns(__mtx.try_lock()), _M_context(__context), _M_chkTimeout(__chkTimeout) {
		assert(_M_context);
	}
	contexted_shared_lock(contexted_shared_lock&& lck)
		: _M_mtx(lck._M_mtx), _M_owns(lck._M_owns), _M_context(lck._M_context), _M_chkTimeout(lck._M_chkTimeout) {
		lck._M_owns = false;
		lck._M_mtx = nullptr;
		lck._M_context = nullptr;
	}
	~contexted_shared_lock() {
		if (_M_owns) _M_mtx->unlock_shared();
	}

	contexted_shared_lock(const contexted_shared_lock&) = delete;
	contexted_shared_lock& operator=(const contexted_shared_lock&) = delete;
	contexted_shared_lock& operator=(contexted_shared_lock&& lck) {
		if (this != &lck) {
			if (_M_owns) unlock();
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
		_M_lockable();
		assert(_M_context);
		const auto lockWard = _M_context->BeforeLock(_Mutex::mark);
		if (_M_chkTimeout.count() > 0 && _M_context->isCancelable()) {
			do {
				ThrowOnCancel(*_M_context, "Lock was canceled on condition"_sv);
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

}  // namespace reindexer
