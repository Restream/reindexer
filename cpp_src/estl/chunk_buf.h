#pragma once

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <string_view>
#include <vector>
#include "chunk.h"
#include "span.h"
#include "tools/errors.h"

namespace reindexer {

template <typename Mutex>
class chain_buf {
public:
	chain_buf(size_t cap) : ring_(cap) {}
	void write(chunk &&ch) {
		if (ch.size()) {
			std::lock_guard lck(mtx_);
			const auto new_head = (head_ + 1) % ring_.size();
			if (new_head == tail_) {
				throw Error(errLogic, "Chain buffer overflow (max size is %d)", ring_.size());
			}
			data_size_ += ch.size();
			ring_[head_] = std::move(ch);
			head_ = new_head;
		}
	}
	void write(std::string_view sv) {
		chunk chunk = get_chunk();
		chunk.append(sv);
		write(std::move(chunk));
	}
	span<chunk> tail() {
		std::lock_guard lck(mtx_);
		size_t cnt = ((tail_ > head_) ? ring_.size() : head_) - tail_;
		return span<chunk>(ring_.data() + tail_, cnt);
	}
	void erase(size_t nread) {
		std::lock_guard lck(mtx_);
		nread = std::min(data_size_, nread);
		data_size_ -= nread;
		while (nread) {
			assertrx(head_ != tail_);
			chunk &cur = ring_[tail_];
			if (cur.size() > nread) {
				cur.offset_ += nread;
				break;
			}
			nread -= cur.size();
			cur.len_ = 0;
			cur.offset_ = 0;
			if (free_.size() < ring_.size() && cur.cap_ < 0x10000)
				free_.push_back(std::move(cur));
			else
				cur = chunk();
			tail_ = (tail_ + 1) % ring_.size();
		}
	}
	chunk get_chunk() {
		std::lock_guard lck(mtx_);
		chunk ret;
		if (free_.size()) {
			ret = std::move(free_.back());
			free_.pop_back();
		}
		return ret;
	}

	size_t size() {
		std::lock_guard lck(mtx_);
		return (head_ - tail_ + ring_.size()) % ring_.size();
	}

	size_t data_size() {
		std::lock_guard lck(mtx_);
		return data_size_;
	}

	size_t capacity() {
		std::lock_guard lck(mtx_);
		return ring_.size() - 1;
	}
	void clear() {
		std::lock_guard lck(mtx_);
		head_ = tail_ = data_size_ = 0;
	}

protected:
	size_t head_ = 0, tail_ = 0, data_size_ = 0;
	std::vector<chunk> ring_, free_;
	Mutex mtx_;
};

}  // namespace reindexer
