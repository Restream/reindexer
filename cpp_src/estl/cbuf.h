#pragma once

#include <algorithm>
#include <climits>
#include <memory>
#include <span>
#include "tools/errors.h"

namespace reindexer {

template <typename T>
class [[nodiscard]] cbuf {
public:
	cbuf(size_t bufsize = 0) : buf_(new T[bufsize]) {
		head_ = 0;
		tail_ = 0;
		full_ = false;
		buf_size_ = bufsize;
	}
	cbuf(cbuf&& other) noexcept
		: head_(other.head_), tail_(other.tail_), full_(other.full_), buf_size_(other.buf_size_), buf_(std::move(other.buf_)) {
		other.head_ = 0;
		other.tail_ = 0;
		other.full_ = false;
		other.buf_size_ = 0;
	}
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
	size_t read(T* p_ins, size_t s_ins) {
		if (s_ins > size()) {
			s_ins = size();
		}

		if (!s_ins) {
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
	size_t peek(T* p_ins, size_t s_ins) {
		if (s_ins > size()) {
			s_ins = size();
		}

		if (!s_ins) {
			return 0;
		}

		size_t lSize = buf_size_ - tail_;

		std::copy(&buf_[tail_], &buf_[tail_ + std::min(s_ins, lSize)], p_ins);
		if (s_ins > lSize) {
			std::copy(&buf_[0], &buf_[s_ins - lSize], p_ins + lSize);
		}

		return s_ins;
	}

	size_t erase(size_t s_erase) noexcept {
		assertf(s_erase <= size(), "s_erase={}, size()={}, tail={},head={},full={}", int(s_erase), int(size()), int(tail_), int(head_),
				int(full_));

		tail_ = (tail_ + s_erase) % buf_size_;
		full_ = full_ && (s_erase == 0);
		return s_erase;
	}
	void clear() noexcept {
		head_ = 0;
		tail_ = 0;
		full_ = 0;
	}

	size_t size() noexcept {
		std::ptrdiff_t D = head_ - tail_;
		if (D < 0 || (D == 0 && full_)) {
			D += buf_size_;
		}
		return D;
	}

	size_t capacity() const noexcept { return buf_size_; }

	std::span<T> tail(size_t s_ins = INT_MAX) noexcept {
		size_t cnt = ((tail_ > head_ || full_) ? buf_size_ : head_) - tail_;
		return std::span<T>(&buf_[tail_], (cnt > s_ins) ? s_ins : cnt);
	}

	std::span<T> head(size_t s_ins = INT_MAX) noexcept {
		size_t cnt = ((head_ >= tail_ && !full_) ? buf_size_ : tail_) - head_;
		return std::span<T>(&buf_[head_], (cnt > s_ins) ? s_ins : cnt);
	}

	void advance_head(size_t cnt) noexcept {
		if (cnt) {
			head_ = (head_ + cnt) % buf_size_;
			full_ = (head_ == tail_);
		}
	}

	void unroll() { grow(0); }
	size_t available() noexcept { return (buf_size_ - size()); }
	void reserve(size_t sz) {
		if (sz > capacity()) {
			grow(sz - capacity());
		}
	}

protected:
	void grow(size_t sz) {
		size_t new_size = buf_size_ + sz;
		size_t lSize = buf_size_ - tail_;
		sz = size();

		std::unique_ptr<T[]> new_buf(new T[new_size]);
		std::copy(&buf_[tail_], &buf_[tail_ + std::min(sz, lSize)], &new_buf[0]);
		if (sz > lSize) {
			std::copy(&buf_[0], &buf_[head_], &new_buf[lSize]);
		}

		tail_ = 0;
		head_ = sz % new_size;
		full_ = (sz == new_size);
		buf_ = std::move(new_buf);
		buf_size_ = new_size;
	}

	size_t head_, tail_, buf_size_;
	bool full_;
	std::unique_ptr<T[]> buf_;
};

}  // namespace reindexer
