#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <span>
#include <type_traits>

#include "estl/defines.h"
#include "tools/assertrx.h"

namespace reindexer::unaligned {

template <typename T>
RX_ALWAYS_INLINE T read(const void* p) noexcept {
	static_assert(std::is_trivially_copyable_v<T>);
	T v;
	std::memcpy(&v, p, sizeof(T));
	return v;
}

template <typename T>
RX_ALWAYS_INLINE void write(void* p, const T& v) noexcept {
	static_assert(std::is_trivially_copyable_v<T>);
	std::memcpy(p, &v, sizeof(T));
}

template <typename T>
	requires std::is_trivially_copyable_v<T>
class [[nodiscard]] view {
public:
	static_assert(sizeof(T) % alignof(T) == 0, "This is required for isAligned_-flag stability across all the range");

	class [[nodiscard]] iterator {
	public:
		using iterator_category = std::random_access_iterator_tag;
		using value_type = T;
		using difference_type = std::ptrdiff_t;
		using pointer = void;
		using reference = T;

		constexpr iterator() noexcept = default;
		constexpr iterator(const uint8_t* ptr, bool isAligned) noexcept : ptr_(ptr), isAligned_(isAligned) {}

		constexpr T operator*() const noexcept { return read_elem(ptr_, isAligned_); }

		constexpr iterator& operator++() noexcept {
			ptr_ += sizeof(T);
			return *this;
		}
		constexpr iterator operator++(int) noexcept {
			iterator tmp = *this;
			++*this;
			return tmp;
		}
		constexpr iterator& operator--() noexcept {
			ptr_ -= sizeof(T);
			return *this;
		}
		constexpr iterator operator--(int) noexcept {
			iterator tmp = *this;
			--*this;
			return tmp;
		}

		constexpr iterator& operator+=(difference_type n) noexcept {
			ptr_ += n * static_cast<difference_type>(sizeof(T));
			return *this;
		}
		constexpr iterator& operator-=(difference_type n) noexcept {
			ptr_ -= n * static_cast<difference_type>(sizeof(T));
			return *this;
		}

		friend constexpr iterator operator+(iterator it, difference_type n) noexcept {
			it += n;
			return it;
		}
		friend constexpr iterator operator+(difference_type n, iterator it) noexcept {
			it += n;
			return it;
		}
		friend constexpr iterator operator-(iterator it, difference_type n) noexcept {
			it -= n;
			return it;
		}
		friend constexpr difference_type operator-(iterator lhs, iterator rhs) noexcept {
			return (lhs.ptr_ - rhs.ptr_) / static_cast<difference_type>(sizeof(T));
		}

		constexpr T operator[](difference_type n) const noexcept {
			return read_elem(ptr_ + n * static_cast<difference_type>(sizeof(T)), isAligned_);
		}

		friend constexpr bool operator==(iterator lhs, iterator rhs) noexcept { return lhs.ptr_ == rhs.ptr_; }
		friend constexpr bool operator!=(iterator lhs, iterator rhs) noexcept { return lhs.ptr_ != rhs.ptr_; }
		friend constexpr bool operator<(iterator lhs, iterator rhs) noexcept { return lhs.ptr_ < rhs.ptr_; }
		friend constexpr bool operator>(iterator lhs, iterator rhs) noexcept { return lhs.ptr_ > rhs.ptr_; }
		friend constexpr bool operator<=(iterator lhs, iterator rhs) noexcept { return lhs.ptr_ <= rhs.ptr_; }
		friend constexpr bool operator>=(iterator lhs, iterator rhs) noexcept { return lhs.ptr_ >= rhs.ptr_; }

	private:
		const uint8_t* ptr_ = nullptr;
		bool isAligned_ = false;
	};

	using element_type = T;
	using value_type = T;
	using size_type = std::size_t;
	using difference_type = std::ptrdiff_t;
	using const_iterator = iterator;

	constexpr view() noexcept = default;

	constexpr view(const void* data, size_type count) noexcept : view(static_cast<const uint8_t*>(data), count, check_aligned(data)) {}

	constexpr view(std::span<const T> s) noexcept : view(reinterpret_cast<const uint8_t*>(s.data()), s.size(), check_aligned(s.data())) {}

	constexpr size_type size() const noexcept { return size_; }
	constexpr bool empty() const noexcept { return size_ == 0; }
	constexpr bool is_aligned() const noexcept { return isAligned_; }

	constexpr T operator[](size_type index) const noexcept { return read_elem(data_ + index * sizeof(T), isAligned_); }

	constexpr T front() const noexcept {
		assertrx(!empty());
		return (*this)[0];
	}
	constexpr T back() const noexcept {
		assertrx(!empty());
		return (*this)[size_ - 1];
	}

	constexpr view subspan(size_type offset, size_type count) const noexcept {
		if (offset >= size_) {
			return {};
		}
		const auto boundedCount = std::min(count, size_ - offset);
		const auto* newData = data_ + offset * sizeof(T);
		const bool aligned = isAligned_ && ptr_is_aligned(newData, alignof(T));
		return {newData, boundedCount, aligned};
	}

	constexpr view subspan(size_type offset) const noexcept {
		if (offset >= size_) {
			return {};
		}
		return subspan(offset, size_ - offset);
	}

	constexpr const_iterator begin() const noexcept { return {data_, isAligned_}; }
	constexpr const_iterator end() const noexcept { return {data_ + size_ * sizeof(T), isAligned_}; }

	constexpr const void* bytes() const noexcept { return data_; }

private:
	constexpr view(const uint8_t* data, size_type count, bool isAligned) noexcept : data_(data), size_(count), isAligned_(isAligned) {}

	static constexpr RX_ALWAYS_INLINE bool check_aligned(const void* data) noexcept {
		return data == nullptr || ptr_is_aligned(data, alignof(T));
	}
	static constexpr RX_ALWAYS_INLINE T read_elem(const void* p, bool isAligned) noexcept {
		if (isAligned) {
			return *static_cast<const T*>(p);
		}
		return read<T>(p);
	}
	static constexpr RX_ALWAYS_INLINE bool ptr_is_aligned(const void* p, size_t alignment) noexcept {
		return (reinterpret_cast<uintptr_t>(p) % alignment) == 0;
	}

	const uint8_t* data_ = nullptr;
	size_type size_ = 0;
	bool isAligned_ = false;
};

}  // namespace reindexer::unaligned
