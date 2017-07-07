#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <climits>

namespace reindexer {

template <typename T>
class cbuf {
public:
	struct chunk {
		T *data;
		size_t len;
	};

	cbuf(size_t bufsize) {
		head_ = 0;
		tail_ = 0;
		full_ = false;
		buf_size_ = bufsize;
		buf_ = new T[buf_size_];
	}

	~cbuf() { delete[] buf_; }

	size_t write(const T *p_ins, size_t s_ins) {
		if (s_ins > available()) grow(s_ins - available());

		if (!s_ins) return 0;

		size_t lSize = buf_size_ - head_;

		memcpy(buf_ + head_, p_ins, std::min(s_ins, lSize) * sizeof(T));

		if (s_ins > lSize) memcpy(buf_, p_ins + lSize, (s_ins - lSize) * sizeof(T));

		head_ = (head_ + s_ins) % buf_size_;
		full_ = (head_ == tail_);
		return s_ins;
	}
	size_t read(T *p_ins, size_t s_ins) {
		if (s_ins > size()) s_ins = size();

		if (!s_ins) return 0;

		size_t lSize = buf_size_ - tail_;

		memcpy(p_ins, buf_ + tail_, std::min(s_ins, lSize) * sizeof(T));

		if (s_ins > lSize) memcpy(p_ins + lSize, buf_, (s_ins - lSize));

		tail_ = (tail_ + s_ins) % buf_size_;
		full_ = false;
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

	chunk tail(size_t s_ins = INT_MAX) {
		size_t cnt = ((tail_ > head_ || full_) ? buf_size_ : head_) - tail_;
		return chunk{buf_ + tail_, (cnt > s_ins) ? s_ins : cnt};
	}

	chunk head(size_t s_ins = INT_MAX) {
		size_t cnt = ((head_ >= tail_ && !full_) ? buf_size_ : tail_) - head_;
		return chunk{buf_ + head_, (cnt > s_ins) ? s_ins : cnt};
	}

	size_t advance_head(size_t cnt) {
		if (cnt) {
			head_ = (head_ + cnt) % buf_size_;
			full_ = (head_ == tail_);
		}
		return cnt;
	}
	void unroll() { grow(0); }

protected:
	void grow(size_t sz) {
		size_t new_size = buf_size_ + sz;
		T *new_buf = new T[new_size];
		sz = size();
		read(new_buf, size());
		delete[] buf_;
		tail_ = 0;
		head_ = sz % new_size;
		full_ = (sz == new_size);
		buf_ = new_buf;
		buf_size_ = new_size;
	};
	size_t available() { return (buf_size_ - size()); }

	size_t head_, tail_, buf_size_;
	bool full_;
	T *buf_;
};

}  // namespace reindexer

/// |          ht     |   |                        |