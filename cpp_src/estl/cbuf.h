#pragma once

#include <algorithm>
#include <climits>
#include <memory>
#include <new>
#include <span>
#include "tools/assertrx.h"
#include "tools/errors.h"

namespace reindexer {

/// Circular byte/element ring buffer.
/// Maintains a write position (`head_`) and a read position (`tail_`).
/// A full ring (`size() == capacity()`) has an empty `head()` span.
/// @note `compact` and `unroll` reallocate and linearize the buffer, which invalidates all spans returned by `head()` or `tail()`.
template <typename T>
class [[nodiscard]] cbuf {
public:
	/// Constructs a circular buffer with the specified capacity.
	cbuf(size_t bufsize = 0) : head_(0), tail_(0), buf_size_(bufsize), full_(false), buf_(new T[bufsize]) {}
	/// Move constructor. Leaves the source empty (capacity 0).
	cbuf(cbuf&& other) noexcept
		: head_(other.head_), tail_(other.tail_), buf_size_(other.buf_size_), full_(other.full_), buf_(std::move(other.buf_)) {
		other.head_ = 0;
		other.tail_ = 0;
		other.full_ = false;
		other.buf_size_ = 0;
	}
	/// Move assignment. Leaves the source empty (capacity 0).
	cbuf& operator=(cbuf&& other) noexcept {
		if (this != &other) {
			buf_ = std::move(other.buf_);
			head_ = other.head_;
			tail_ = other.tail_;
			full_ = other.full_;
			buf_size_ = other.buf_size_;
			other.head_ = 0;
			other.tail_ = 0;
			other.full_ = false;
			other.buf_size_ = 0;
		}
		return *this;
	}
	cbuf(const cbuf&) = delete;
	cbuf& operator=(const cbuf&) = delete;

	/// Writes data into the ring buffer, growing capacity if necessary.
	/// @param p_ins - pointer to the data to write
	/// @param s_ins - number of elements to write
	/// @return number of elements written
	size_t write(const T* p_ins, size_t s_ins) {
		if (s_ins > available()) {
			grow(std::max(s_ins - available(), buf_size_));
		}

		if (!s_ins) {
			return 0;
		}

		size_t lSize = buf_size_ - head_;

		std::copy(p_ins, p_ins + std::min(s_ins, lSize), &buf_[head_]);
		if (s_ins > lSize) {
			std::copy(p_ins + lSize, p_ins + s_ins, &buf_[0]);
		}

		head_ = (head_ + s_ins) % buf_size_;
		full_ = (head_ == tail_);
		return s_ins;
	}

	/// Reads and consumes data from the `tail()` of the ring buffer.
	/// @param p_ins - pointer to the destination buffer
	/// @param s_ins - number of elements to read
	/// @return number of elements actually read
	size_t read(T* p_ins, size_t s_ins) {
		if (s_ins > size()) {
			s_ins = size();
		}

		if (!s_ins || !p_ins) [[unlikely]] {
			return 0;
		}

		size_t lSize = buf_size_ - tail_;

		std::copy(&buf_[tail_], &buf_[tail_ + std::min(s_ins, lSize)], p_ins);
		if (s_ins > lSize) {
			std::copy(&buf_[0], &buf_[s_ins - lSize], p_ins + lSize);
		}

		tail_ = (tail_ + s_ins) % buf_size_;
		full_ = false;
		return s_ins;
	}

	/// Reads data from the `tail()` of the ring buffer without consuming it.
	/// @param p_ins - pointer to the destination buffer
	/// @param s_ins - number of elements to peek
	/// @return number of elements actually peeked
	size_t peek(T* p_ins, size_t s_ins) {
		if (s_ins > size()) {
			s_ins = size();
		}

		if (!s_ins || !p_ins) [[unlikely]] {
			return 0;
		}

		size_t lSize = buf_size_ - tail_;

		std::copy(&buf_[tail_], &buf_[tail_ + std::min(s_ins, lSize)], p_ins);
		if (s_ins > lSize) {
			std::copy(&buf_[0], &buf_[s_ins - lSize], p_ins + lSize);
		}

		return s_ins;
	}

	/// Consumes data from the `tail()` without copying.
	/// @param s_erase - number of elements to erase. Must be <= `size()`.
	/// @return number of elements erased
	size_t erase(size_t s_erase) noexcept {
		assertf(s_erase <= size(), "s_erase={}, size()={}, tail={},head={},full={}", int(s_erase), int(size()), int(tail_), int(head_),
				int(full_));

		tail_ = (tail_ + s_erase) % buf_size_;
		full_ = full_ && (s_erase == 0);
		return s_erase;
	}

	/// Clears the buffer, resetting read and write positions.
	/// Does not shrink capacity.
	void clear() noexcept {
		head_ = 0;
		tail_ = 0;
		full_ = 0;
	}

	/// Occupied element count.
	size_t size() noexcept {
		std::ptrdiff_t D = head_ - tail_;
		if (D < 0 || (D == 0 && full_)) {
			D += buf_size_;
		}
		return D;
	}

	/// Maximum elements the ring can hold before growing.
	size_t capacity() const noexcept { return buf_size_; }

	/// Occupied data at the read position (contiguous; not the full payload when wrapped).
	/// Empty if capacity is 0 (`buf_` is not dereferenced).
	/// @param s_ins - max length of the returned span
	std::span<T> tail(size_t s_ins = INT_MAX) noexcept {
		if (!buf_size_) {
			return {};
		}
		size_t cnt = ((tail_ > head_ || full_) ? buf_size_ : head_) - tail_;
		return std::span<T>(&buf_[tail_], (cnt > s_ins) ? s_ins : cnt);
	}

	/// Free space at the write position (contiguous; empty if the ring is full).
	/// Empty if capacity is 0 (`buf_` is not dereferenced).
	/// @param s_ins - max length of the returned span
	std::span<T> head(size_t s_ins = INT_MAX) noexcept {
		if (!buf_size_) {
			return {};
		}
		size_t cnt = ((head_ >= tail_ && !full_) ? buf_size_ : tail_) - head_;
		return std::span<T>(&buf_[head_], (cnt > s_ins) ? s_ins : cnt);
	}

	/// Commits elements placed into the `head()` span.
	/// @param cnt - number of elements to commit
	void advance_head(size_t cnt) noexcept {
		if (cnt) {
			head_ = (head_ + cnt) % buf_size_;
			full_ = (head_ == tail_);
		}
	}

	/// Linearizes the buffer at the same capacity (no growth).
	/// Invalidates all `head()` and `tail()` spans.
	void unroll() { compact(buf_size_); }

	/// Free element count before the ring is full.
	size_t available() noexcept { return (buf_size_ - size()); }

	/// Ensures the buffer has at least the specified capacity.
	void reserve(size_t sz) {
		if (sz > capacity()) {
			grow(sz - capacity());
		}
	}

	/// Shrinks the buffer capacity. Does not grow. Resulting capacity is max(targetCapacity, size()).
	/// Best-effort: on allocation failure keeps the current buffer and returns false.
	/// @param targetCapacity - desired capacity
	/// @return true if successful, false on allocation failure
	bool shrink(size_t targetCapacity) noexcept {
		const auto used = size();
		const auto newCapacity = std::max(targetCapacity, used);
		if (newCapacity >= buf_size_) {
			return true;
		}
		return tryCompact(newCapacity);
	}

	/// Shrinks the buffer if its capacity exceeds a threshold.
	/// @param keep_cap - capacity to shrink to (must be <= threshold)
	/// @param threshold - capacity threshold above which to trigger shrinking
	/// @return true if successful or not needed, false on allocation failure
	bool shrink_if_needed(size_t keep_cap, size_t threshold) noexcept {
		assertrx_dbg(keep_cap <= threshold);
		if (capacity() > threshold && size() < keep_cap) {
			return shrink(keep_cap);
		}
		return true;
	}

private:
	/// Grows the buffer by the specified additional capacity and linearizes it.
	void grow(size_t sz) { compact(buf_size_ + sz); }

	/// Reallocates and linearizes the buffer (`tail_ = 0`).
	/// Invalidates all `head()` and `tail()` spans.
	void compact(size_t newCapacity) {
		if (!tryCompact(newCapacity)) {
			throw std::bad_alloc();
		}
	}

	/// Attempts to reallocate and linearize the buffer.
	/// @return true on success, false on allocation failure
	bool tryCompact(size_t newCapacity) noexcept {
		const size_t used = size();
		assertrx_dbg(newCapacity >= used);
		if (newCapacity == buf_size_ && tail_ == 0) {
			return true;
		}
		if (newCapacity == 0) {
			buf_.reset();
			head_ = 0;
			tail_ = 0;
			full_ = false;
			buf_size_ = 0;
			return true;
		}

		std::unique_ptr<T[]> newBuf(new (std::nothrow) T[newCapacity]);
		if (!newBuf) {
			return false;
		}
		if (used) {
			const size_t first = std::min(used, buf_size_ - tail_);
			std::copy(&buf_[tail_], &buf_[tail_ + first], newBuf.get());
			if (used > first) {
				std::copy(&buf_[0], &buf_[head_], newBuf.get() + first);
			}
		}
		tail_ = 0;
		head_ = (used == newCapacity) ? 0 : used;
		full_ = (used == newCapacity);
		buf_ = std::move(newBuf);
		buf_size_ = newCapacity;
		return true;
	}

	size_t head_, tail_, buf_size_;
	bool full_;
	std::unique_ptr<T[]> buf_;
};

}  // namespace reindexer
