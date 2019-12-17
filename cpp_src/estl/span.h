#pragma once

#include <assert.h>
#include <stdint.h>
#include "string_view.h"
#include "trivial_reverse_iterator.h"

namespace reindexer {
template <typename T>
class span {
public:
	typedef T value_type;
	typedef T* pointer;
	typedef const T* const_pointer;
	typedef const_pointer const_iterator;
	typedef pointer iterator;
	typedef trivial_reverse_iterator<const_iterator> const_reverse_iterator;
	typedef trivial_reverse_iterator<iterator> reverse_iterator;
	typedef size_t size_type;

	constexpr span() noexcept : data_(nullptr), size_(0) {}
	constexpr span(const span& other) noexcept : data_(other.data_), size_(other.size_) {}

	span& operator=(const span& other) noexcept {
		data_ = other.data_;
		size_ = other.size_;
		return *this;
	}

	span& operator=(span&& other) noexcept {
		data_ = other.data_;
		size_ = other.size_;
		return *this;
	}

	// FIXME: const override
	template <typename Container>
	constexpr span(const Container& other) noexcept : data_(const_cast<T*>(other.data())), size_(other.size()) {}

	constexpr span(const T* str, size_type len) : data_(const_cast<T*>(str)), size_(len) {}  // static??
	constexpr iterator begin() const noexcept { return data_; }
	constexpr iterator end() const noexcept { return data_ + size_; }
	/*constexpr*/ reverse_iterator rbegin() const noexcept {
		reverse_iterator it;
		it = end();
		return it;
	}
	/*constexpr*/ reverse_iterator rend() const noexcept {
		reverse_iterator it;
		it = begin();
		return it;
	}
	constexpr size_type size() const noexcept { return size_; }
	constexpr bool empty() const noexcept { return size_ == 0; }
	constexpr const T& operator[](size_type pos) const { return data_[pos]; }
	T& operator[](size_type pos) { return data_[pos]; }
	constexpr const T& at(size_type pos) const { return data_[pos]; }
	constexpr const T& front() const { return data_[0]; }
	constexpr const T& back() const { return data_[size() - 1]; }
	T& at(size_type pos) { return data_[pos]; }
	T& front() { return data_[0]; }
	T& back() { return data_[size() - 1]; }

	constexpr pointer data() const noexcept { return data_; }
	span subspan(size_type offset, size_type count) const noexcept {
		assert(offset + count <= size_);
		return span(data_ + offset, count);
	}
	bool operator==(const span& other) const noexcept {
		if (&other != this) {
			if (size() != other.size()) return false;
			for (size_t i = 0; i < size(); ++i) {
				if (!(at(i) == other.at(i))) return false;
			}
			return true;
		}
		return true;
	}
	bool operator!=(const span& other) const noexcept { return !operator==(other); }

protected:
	pointer data_;
	size_type size_;
};

inline static span<char> giftStr(string_view sv) { return span<char>(const_cast<char*>(sv.data()), sv.size()); }

}  // namespace reindexer
