#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <climits>
#include <vector>
#include "h_vector.h"

namespace reindexer {

template <typename T>
class cbuf {
public:
	cbuf(size_t bufsize = 0) : buf_(bufsize) {
		head_ = 0;
		tail_ = 0;
		full_ = false;
		buf_size_ = bufsize;
	}
	cbuf(cbuf &&other)
		: head_(other.head_), tail_(other.tail_), full_(other.full_), buf_size_(other.buf_size_), buf_(std::move(other.buf_)) {
		other.head_ = 0;
		other.tail_ = 0;
		other.full_ = false;
		other.buf_size_ = 0;
	}
	cbuf &operator=(cbuf &&other) {
		if (this != &other) {
			buf_ = std::move(other.buf_);
			head_ = other.head_;
			tail_ = other.tail_;
			full_ = other.full_;
			buf_size_ = other.buf_size_;
			buf_ = other.buf_;
			other.head_ = 0;
			other.tail_ = 0;
			other.full_ = false;
			other.buf_size_ = 0;
		}
		return *this;
	}
	cbuf(const cbuf &) = delete;
	cbuf &operator=(const cbuf &) = delete;

	~cbuf() {}

	size_t write(const T *p_ins, size_t s_ins) {
		if (s_ins > available()) grow(std::max(s_ins - available(), buf_size_));

		if (!s_ins) return 0;

		size_t lSize = buf_size_ - head_;

		std::copy(p_ins, p_ins + std::min(s_ins, lSize), &buf_[head_]);
		if (s_ins > lSize) std::copy(p_ins + lSize, p_ins + s_ins, &buf_[0]);

		head_ = (head_ + s_ins) % buf_size_;
		full_ = (head_ == tail_);
		return s_ins;
	}
	size_t read(T *p_ins, size_t s_ins) {
		if (s_ins > size()) s_ins = size();

		if (!s_ins) return 0;

		size_t lSize = buf_size_ - tail_;

		std::copy(&buf_[tail_], &buf_[tail_ + std::min(s_ins, lSize)], p_ins);
		if (s_ins > lSize) std::copy(&buf_[0], &buf_[s_ins - lSize], p_ins + lSize);

		tail_ = (tail_ + s_ins) % buf_size_;
		full_ = false;
		return s_ins;
	}
	size_t peek(T *p_ins, size_t s_ins) {
		if (s_ins > size()) s_ins = size();

		if (!s_ins) return 0;

		size_t lSize = buf_size_ - tail_;

		std::copy(&buf_[tail_], &buf_[tail_ + std::min(s_ins, lSize)], p_ins);
		if (s_ins > lSize) std::copy(&buf_[0], &buf_[s_ins - lSize], p_ins + lSize);

		return s_ins;
	}

	size_t erase(size_t s_erase, bool from_back = false) {
		if (s_erase > size()) s_erase = size();

		if (from_back)
			head_ = (head_ + buf_size_ - s_erase) % buf_size_;
		else
			tail_ = (tail_ + s_erase) % buf_size_;
		full_ = full_ && (s_erase == 0);
		return s_erase;
	}
	void clear() {
		head_ = 0;
		tail_ = 0;
		full_ = 0;
	}

	size_t size() {
		int D = head_ - tail_;
		if (D < 0 || (D == 0 && full_)) D += buf_size_;
		return D;
	}

	size_t capacity() const { return buf_size_; }

	span<T> tail(size_t s_ins = INT_MAX) {
		size_t cnt = ((tail_ > head_ || full_) ? buf_size_ : head_) - tail_;
		return span<T>(buf_.data() + tail_, (cnt > s_ins) ? s_ins : cnt);
	}

	span<T> head(size_t s_ins = INT_MAX) {
		size_t cnt = ((head_ >= tail_ && !full_) ? buf_size_ : tail_) - head_;
		return span<T>(buf_.data() + head_, (cnt > s_ins) ? s_ins : cnt);
	}

	size_t advance_head(size_t cnt) {
		if (cnt) {
			head_ = (head_ + cnt) % buf_size_;
			full_ = (head_ == tail_);
		}
		return cnt;
	}
	void unroll() { grow(0); }
	size_t available() { return (buf_size_ - size()); }
	void reserve(size_t sz) {
		if (sz > capacity()) grow(sz - capacity());
	}

protected:
	void grow(size_t sz) {
		size_t new_size = buf_size_ + sz;
		size_t lSize = buf_size_ - tail_;
		sz = size();

		std::vector<T> new_buf;
		new_buf.reserve(new_size);
		new_buf.insert(new_buf.begin(), &buf_[tail_], &buf_[tail_ + std::min(sz, lSize)]);
		if (sz > lSize) new_buf.insert(new_buf.end(), &buf_[0], &buf_[sz - lSize]);

		tail_ = 0;
		head_ = sz % new_size;
		full_ = (sz == new_size);
		buf_ = std::move(new_buf);
		buf_size_ = new_size;
	};

	size_t head_, tail_, buf_size_;
	bool full_;
	std::vector<T> buf_;
};

}  // namespace reindexer
