#pragma once

#include <cassert>
#include <condition_variable>
#include <string_view>
#include "contexted_locks.h"

namespace reindexer {

class [[nodiscard]] contexted_cond_var {
public:
	using CondVarType = std::condition_variable_any;

	explicit contexted_cond_var(milliseconds __chk_timeout = kDefaultCondChkTime)
		: _M_cond_var(new CondVarType), _M_chk_timeout(__chk_timeout) {}
	contexted_cond_var(contexted_cond_var&& other) = delete;
	contexted_cond_var(const contexted_cond_var&) = delete;
	contexted_cond_var& operator=(const contexted_cond_var&) = delete;
	contexted_cond_var& operator=(contexted_cond_var&& other) = delete;

	template <typename _Lock, typename _Predicate, typename _ContextT>
	void wait(_Lock& __lock, _Predicate __p, const _ContextT& __context) {
		using namespace std::string_view_literals;
		assert(_M_cond_var);
		// const auto lockWard = _M_context->BeforeLock(_Mutex::mark);
		if (_M_chk_timeout.count() > 0 && __context.IsCancelable()) {
			while (!_M_cond_var->wait_for(__lock, _M_chk_timeout, __p)) {
				ThrowOnCancel(__context, "Context was canceled or timed out (condition variable)"sv);
			}
		} else {
			_M_cond_var->wait(__lock, std::move(__p));
		}
	}

	void notify_all() {
		assert(_M_cond_var);
		_M_cond_var->notify_all();
	}

	void notify_one() {
		assert(_M_cond_var);
		_M_cond_var->notify_one();
	}

private:
	std::unique_ptr<CondVarType> _M_cond_var;
	milliseconds _M_chk_timeout;
};

}  // namespace reindexer
