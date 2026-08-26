#pragma once

#include <algorithm>
#include <cstdlib>
#include <span>
#include <string_view>
#include <vector>
#include "chunk.h"
#include "lock.h"
#include "tools/assertrx.h"
#include "tools/errors.h"

namespace reindexer {

template <typename Mutex>
class [[nodiscard]] chain_buf {
public:
	chain_buf(size_t cap) : ring_(cap) {}
	void write(chunk&& ch) {
		if (ch.size()) {
			lock_guard lck(mtx_);
			const auto new_head = (head_ + 1) % ring_.size();
			if (new_head == tail_) {
				throw Error(errLogic, "Chain buffer overflow (max size is {})", ring_.size());
			}
			data_size_ += ch.size();
			ring_[head_] = std::move(ch);
			head_ = new_head;
			size_.fetch_add(1, std::memory_order_acq_rel);
			assertrx_dbg(size_atomic() == size_impl());
		}
	}
	void write(std::string_view sv) {
		chunk chunk = get_chunk();
		chunk.append(sv);
		write(std::move(chunk));
	}
	std::span<chunk> tail() noexcept {
		lock_guard lck(mtx_);
		size_t cnt = ((tail_ > head_) ? ring_.size() : head_) - tail_;
		return std::span<chunk>(ring_.data() + tail_, cnt);
	}
	void erase(size_t nread) {
		lock_guard lck(mtx_);
		nread = std::min(data_size_, nread);
		data_size_ -= nread;
		while (nread) {
			assertrx(head_ != tail_);
			chunk& cur = ring_[tail_];
			if (cur.size() > nread) {
				cur.shift(nread);
				break;
			}
			size_.fetch_sub(1, std::memory_order_acq_rel);
			nread -= cur.size();
			recycle(std::move(cur));
			tail_ = (tail_ + 1) % ring_.size();
		}
		assertrx_dbg(size_atomic() == size_impl());
	}
	void erase_chunks(size_t count) {
		lock_guard lck(mtx_);
		const auto erase_count = std::min(count, size_impl());
		for (count = erase_count; count > 0; --count) {
			assertrx(head_ != tail_);
			chunk& cur = ring_[tail_];
			data_size_ -= cur.size();
			recycle(std::move(cur));
			tail_ = (tail_ + 1) % ring_.size();
		}
		size_.fetch_sub(erase_count, std::memory_order_acq_rel);
		assertrx_dbg(size_atomic() == size_impl());
	}
	chunk get_chunk() noexcept {
		chunk ret;
		lock_guard lck(mtx_);
		if (free_.size()) {
			ret = std::move(free_.back());
			free_.pop_back();
		}
		return ret;
	}

	size_t size() const noexcept {
		lock_guard lck(mtx_);
		return size_impl();
	}

	size_t size_atomic() const noexcept { return size_.load(std::memory_order_acquire); }

	size_t data_size() const noexcept {
		lock_guard lck(mtx_);
		return data_size_;
	}

	size_t capacity() const noexcept {
		lock_guard lck(mtx_);
		return ring_.size() - 1;
	}
	void clear() noexcept {
		lock_guard lck(mtx_);
		head_ = tail_ = data_size_ = 0;
		size_.store(0, std::memory_order_release);
		assertrx_dbg(size_atomic() == size_impl());
	}

private:
	size_t size_impl() const noexcept { return (head_ - tail_ + ring_.size()) % ring_.size(); }
	void recycle(chunk&& ch) {
		if (free_.size() < ring_.size() && ch.capacity() < 0x10000) {
			ch.clear();
			free_.emplace_back(std::move(ch));
		} else {
			ch = chunk();
		}
	}

	size_t head_ = 0, tail_ = 0, data_size_ = 0;
	std::atomic<size_t> size_ = {0};
	std::vector<chunk> ring_, free_;
	mutable Mutex mtx_;
};

}  // namespace reindexer
