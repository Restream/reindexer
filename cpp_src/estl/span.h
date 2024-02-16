#pragma once

#include <stdint.h>
#include <string_view>
#include "tools/assertrx.h"
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
	static_assert(std::is_trivial_v<reverse_iterator>, "Expecting trivial reverse iterator");
	static_assert(std::is_trivial_v<const_reverse_iterator>, "Expecting trivial const reverse iterator");

	constexpr span() noexcept : data_(nullptr), size_(0) {}
	constexpr span(const span& other) noexcept : data_(other.data_), size_(other.size_) {}
	constexpr span(span&& other) noexcept : data_(other.data_), size_(other.size_) {}

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

	explicit constexpr span(const T* str, size_type len) noexcept : data_(const_cast<T*>(str)), size_(len) {}
	constexpr span(T* str, size_type len) noexcept : data_(str), size_(len) {}
	template <size_type L>
	constexpr span(T (&arr)[L]) noexcept : data_(arr), size_(L) {}
	constexpr iterator begin() const noexcept { return data_; }
	constexpr iterator end() const noexcept { return data_ + size_; }
	constexpr reverse_iterator rbegin() const noexcept { return end(); }
	constexpr reverse_iterator rend() const noexcept { return begin(); }
	constexpr size_type size() const noexcept { return size_; }
	constexpr bool empty() const noexcept { return size_ == 0; }
	constexpr const T& operator[](size_type pos) const noexcept { return data_[pos]; }
	T& operator[](size_type pos) noexcept { return data_[pos]; }
	constexpr const T& at(size_type pos) const noexcept { return data_[pos]; }
	constexpr const T& front() const noexcept { return data_[0]; }
	constexpr const T& back() const noexcept { return data_[size() - 1]; }
	T& at(size_type pos) noexcept { return data_[pos]; }
	T& front() noexcept { return data_[0]; }
	T& back() noexcept { return data_[size() - 1]; }

	constexpr pointer data() const noexcept { return data_; }
	span subspan(size_type offset, size_type count) const noexcept {
		assertrx(offset + count <= size_);
		return span(data_ + offset, count);
	}
	span subspan(size_type offset) const noexcept {
		assertrx(offset <= size_);
		return span(data_ + offset, size_ - offset);
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

inline span<char> giftStr(std::string_view sv) noexcept { return span<char>(const_cast<char*>(sv.data()), sv.size()); }

}  // namespace reindexer
