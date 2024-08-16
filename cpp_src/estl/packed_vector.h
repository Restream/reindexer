#pragma once

#include <cstdint>
#include <vector>

#include "tools/assertrx.h"

namespace reindexer {
template <typename T>
class packed_vector {
public:
	typedef T value_type;
	typedef unsigned size_type;
	typedef T* pointer;
	typedef T& reference;
	typedef const T* const_pointer;
	typedef const T& const_reference;

	using store_container = std::vector<uint8_t>;
	packed_vector() noexcept : size_(0) {}
	class iterator {
	public:
		iterator(const packed_vector* pv, store_container::const_iterator it) : pv_(pv), it_(it), unpacked_(0) { unpack(); }

		iterator& operator++() {
			unpack();
			it_ += unpacked_;
			unpacked_ = 0;
			return *this;
		}
		pointer operator->() { return &unpack(); }
		reference operator*() { return unpack(); }
		bool operator!=(const iterator& rhs) const noexcept { return it_ != rhs.it_; }
		bool operator==(const iterator& rhs) const noexcept { return it_ == rhs.it_; }
		size_type pos() noexcept { return it_ - pv_->data_.begin(); }

	protected:
		reference unpack() {
			if (!unpacked_ && it_ != pv_->data_.end()) {
				unpacked_ = cur_.unpack(&*it_, pv_->data_.end() - it_);
			}
			return cur_;
		}
		value_type cur_;
		const packed_vector* pv_;
		store_container::const_iterator it_;
		size_type unpacked_;
	};

	using const_iterator = const iterator;
	iterator begin() const { return iterator(this, data_.begin()); }
	iterator end() const { return iterator(this, data_.end()); }

	template <typename TT>
	void push_back(const TT& v) {
		data_.reserve(data_.size() + v.maxpackedsize());
		auto p = v.pack(data_.end());
		data_.resize(p + data_.size());
		size_++;
	}

	void erase_back(size_type pos) {
		for (auto it = iterator(this, data_.begin() + pos); it != end(); ++it) {
			size_--;
		}
		data_.resize(pos);
	}

	size_type size() const noexcept { return size_; }

	template <typename InputIterator>
	void insert(iterator pos, InputIterator from, InputIterator to) {
		assertrx(pos == end());
		(void)pos;
		data_.reserve((to - from) / 2);
		int i = 0;
		size_type p = data_.size();
		for (auto it = from; it != to; it++, i++) {
			if (!(i % 100)) {
				size_type sz = 0, j = 0;
				for (auto iit = it; j < 100 && iit != to; iit++, j++) {
					sz += iit->maxpackedsize();
				}
				data_.resize(p + sz);
			}
			p += it->pack(&*(data_.begin() + p));
			assertrx(p <= data_.size());
		}
		data_.resize(p);
		size_ += (to - from);
	}
	void shrink_to_fit() { data_.shrink_to_fit(); }
	size_type heap_size() noexcept { return data_.capacity(); }
	void clear() noexcept {
		data_.clear();
		size_ = 0;
	}
	bool empty() const noexcept { return size_ == 0; }
	size_type pos(iterator it) noexcept { return it.pos(); }

protected:
	store_container data_;
	size_type size_;
};
}  // namespace reindexer
