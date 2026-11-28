#pragma once

#include <type_traits>
#include "coroutine.h"
#include "estl/h_vector.h"
#include "tools/scope_guard.h"
#include "waiters_queue.h"

namespace reindexer::coroutine {

/// @class Buffered channel, which allows to asynchronously send data between coroutines
/// The behaviour is similar to Golang's buffered channels
template <typename T>
	requires std::is_nothrow_move_constructible_v<T> && std::is_nothrow_default_constructible_v<T>
class [[nodiscard]] channel {
public:
	/// Creates channel with required capacity
	channel(size_t cap = 1) : buf_(cap) {
		if (!capacity()) {
			throw std::logic_error("Empty channels are not allowed");
		}
	}
	channel(const channel&) = delete;
	channel(channel&&) = delete;
	channel& operator=(const channel&) = delete;
	channel& operator=(channel&&) = delete;

	/// Push object to channel.
	/// If channel is full, current coroutine will suspend and wait for pop()-calls from other coroutines.
	/// If channel is closed, exception will be generated.
	/// If there are readers awaiting data, current coroutine will call resume() and switch to those readers.
	/// @param obj - Object to push
	template <typename U>
	void push(U&& obj) {
		assertrx(current());  // For now channels should not be used from main routine dew to current resume/suspend logic
		waiters_queue::hook waiter(current());

		auto waiterGuard = MakeScopeGuard([this, &waiter]() noexcept {
			if (waiter.linked()) {
				writers_.erase(waiter);
			}
		});

#ifdef RX_WITH_STDLIB_DEBUG
		if (!full() && !closed_) {
			// Extra ordering check
			assertrx_dbg(writers_.empty());
		}
#endif	// RX_WITH_STDLIB_DEBUG

		while (full() || closed_) {
			if (closed_) {
				throw std::logic_error("Attempt to write in closed channel");
			}
			if (!waiter.linked()) {
				writers_.push_back(waiter);
			}
			suspend();
		}

		push_impl(std::forward<U>(obj));
		// Unlink before waking readers: a still-linked writer could be cyclically resumed by the reader's pop().
		if (waiter.linked()) {
			writers_.erase(waiter);
		}
		// FIXME: #2558: these resume() calls switch fibers and may run during stack unwinding when a channel op is reached
		// from a destructor on the exception path (e.g. tokens_pool::~token -> return_token() -> push()). Resuming a fiber
		// with an in-flight exception corrupts the per-thread exception machinery
		while (readers_.size() && !empty()) {
			[[maybe_unused]] auto res = resume(readers_.front());
			assertrx_dbg(!res);
		}
	}

	/// Pop object from channel.
	/// If channel is opened and empty, current coroutine will suspend and wait for push()-calls from other coroutines.
	/// If channel is closed and still has data, this data will be returned.
	/// If channel is closed and there are no data to read, the default value will be returned.
	/// If channel is full and there are writers awaiting space in this channel, current coroutine will call resume() and switch to those
	/// writers.
	/// @return Pair of value and flag. Flag shows if it's actual value from channel (true) or default constructed one (false)
	std::pair<T, bool> pop() noexcept {
		assertrx(current());  // For now channels should not be used from main routine dew to current resume/suspend logic
		waiters_queue::hook waiter(current());
		while (empty() && !closed_) {
			if (!waiter.linked()) {
				readers_.push_back(waiter);
			}
			suspend();
		}

		auto obj = pop_impl();
		if (waiter.linked()) {
			readers_.erase(waiter);
		}
		// FIXME: #2558 deferred resume needed on the unwind path here too — see the detailed note in push().
		while (writers_.size() && !full()) {
			[[maybe_unused]] auto res = resume(writers_.front());
			assertrx_dbg(!res);
		}
		return obj;
	}

	/// Close channel.
	/// All reades and writers will be resumed immediately
	void close() noexcept {
		// FIXME: #2558 deferred resume needed on the unwind path here too — see the detailed note in push(). Direct
		// consumers currently call close() from coroutine bodies (or destructors that themselves suspend, hence never run
		// during unwinding), so this is latent rather than active today.
		closed_ = true;
		while (readers_.size()) {
			[[maybe_unused]] auto res = resume(readers_.front());
			assertrx_dbg(!res);
		}
		while (writers_.size()) {
			[[maybe_unused]] auto res = resume(writers_.front());
			assertrx_dbg(!res);
		}
	}
	/// Reopens closed channel
	void reopen() noexcept {
		assertrx(!opened());
		assertrx(writers_.empty());
		assertrx(readers_.empty());
		closed_ = false;
		r_ptr_ = 0;
		w_ptr_ = 0;
		data_size_ = 0;
	}

	size_t size() const noexcept { return data_size_; }
	size_t capacity() const noexcept { return buf_.size(); }
	bool empty() const noexcept { return data_size_ == 0; }
	bool full() const noexcept { return data_size_ == buf_.size(); }
	bool opened() const noexcept { return !closed_; }
	size_t readers() const noexcept { return readers_.size(); }
	size_t writers() const noexcept { return writers_.size(); }

private:
	std::pair<T, bool> pop_impl() noexcept {
		if (data_size_) {
			size_t r_cur = r_ptr_;
			r_ptr_ = (r_ptr_ + 1) % buf_.size();
			--data_size_;
			return std::make_pair(std::move(buf_[r_cur]), true);
		}
		return std::make_pair(T(), false);
	}
	template <typename U>
	void push_impl(U&& obj) {
		buf_[w_ptr_] = std::forward<U>(obj);
		w_ptr_ = (w_ptr_ + 1) % buf_.size();
		++data_size_;
		assertrx(data_size_ <= buf_.size());
	}

	h_vector<T, 2> buf_;
	size_t r_ptr_ = 0;
	size_t w_ptr_ = 0;
	size_t data_size_ = 0;
	waiters_queue writers_;
	waiters_queue readers_;
	bool closed_ = false;
};

}  // namespace reindexer::coroutine
