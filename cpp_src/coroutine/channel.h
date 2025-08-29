#pragma once

#include "coroutine.h"
#include "estl/h_vector.h"

#include <algorithm>

namespace reindexer {
namespace coroutine {

/// @class Buffered channel, which allows to asynchronously send data between coroutines
/// The behaviour is similar to Golang's buffered channels
template <typename T>
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
		bool await = false;

#ifdef RX_WITH_STDLIB_DEBUG
		if (!full() && !closed_) {
			// Extra ordering check
			assertrx_dbg(writers_.empty());
		}
#endif	// RX_WITH_STDLIB_DEBUG

		while (full() || closed_) {
			if (closed_) {
				if (await) {
					remove_waiter(writers_);
				}
				throw std::logic_error("Attempt to write in closed channel");
			}
			if (!await) {
				await = true;
				writers_.emplace_back(current());
			}
			suspend();
		}

		push_impl(std::forward<U>(obj));
		if (await) {
			remove_waiter(writers_);
		}
		while (readers_.size() && !empty()) {
			resume(readers_.front());
		}
	}

	/// Pop object from channel.
	/// If channel is opened and empty, current coroutine will suspend and wait for push()-calls from other coroutines.
	/// If channel is closed and still has data, this data will be returned.
	/// If channel is closed and there are no data to read, the default value will be returned.
	/// If channel is full and there are writers awaiting space in this channel, current coroutine will call resume() and switch to those
	/// writers.
	/// @return Pair of value and flag. Flag shows if it's actual value from channel (true) or default constructed one (false)
	/// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Change waiters_container to intrusive list (allows to avoid allocations)
	std::pair<T, bool> pop() noexcept {
		assertrx(current());  // For now channels should not be used from main routine dew to current resume/suspend logic
		bool await = false;
		while (empty() && !closed_) {
			if (!await) {
				await = true;
				readers_.emplace_back(current());
			}
			suspend();
		}

		auto obj = pop_impl();
		if (await) {
			remove_waiter(readers_);
		}
		while (writers_.size() && !full()) {
			resume(writers_.front());
		}
		return obj;
	}

	/// Close channel.
	/// All reades and writers will be resumed immediately
	void close() noexcept {
		closed_ = true;
		while (readers_.size()) {
			resume(readers_.front());
		}
		while (writers_.size()) {
			resume(writers_.front());
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
	using waiters_container = h_vector<routine_t, 2>;

	std::pair<T, bool> pop_impl() {
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
	static void remove_waiter(waiters_container& waiters) noexcept { waiters.erase(std::find(waiters.begin(), waiters.end(), current())); }

	h_vector<T, 2> buf_;
	size_t r_ptr_ = 0;
	size_t w_ptr_ = 0;
	size_t data_size_ = 0;
	waiters_container writers_;
	waiters_container readers_;
	bool closed_ = false;
};

}  // namespace coroutine
}  // namespace reindexer
