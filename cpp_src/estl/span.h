#pragma once

#include <cstdint>
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

	template <template <typename> class Container, typename U,
			  std::enable_if_t<std::is_same_v<std::remove_const_t<T>, std::remove_const_t<U>> && std::is_const_v<T>, int>* = nullptr>
	constexpr span(const Container<U>& other) noexcept : data_(static_cast<T*>(other.data())), size_(other.size()) {}
	template <template <typename> class Container, typename U,
			  std::enable_if_t<!std::is_const_v<std::remove_reference_t<Container<U>>> &&
								   std::is_same_v<std::remove_const_t<T>, std::remove_const_t<U>>,
							   int>* = nullptr>
	constexpr span(Container<U>&& other) noexcept : data_(static_cast<T*>(other.data())), size_(other.size()) {}
	template <
		typename Container,
		std::enable_if_t<std::is_same_v<std::remove_const_t<T>, std::remove_const_t<typename Container::value_type>> && std::is_const_v<T>,
						 int>* = nullptr>
	constexpr span(const Container& other) noexcept : data_(other.data()), size_(other.size()) {}
	template <typename Container,
			  std::enable_if_t<
				  !std::is_const_v<std::remove_reference_t<Container>> &&
					  std::is_same_v<std::remove_const_t<T>, std::remove_const_t<typename std::remove_reference_t<Container>::value_type>>,
				  int>* = nullptr>
	constexpr span(Container&& other) noexcept : data_(other.data()), size_(other.size()) {}
	template <typename U, std::enable_if_t<std::is_const<T>::value == std::is_const<U>::value, void>* = nullptr>
	explicit constexpr span(U* str, size_type len) noexcept : data_(static_cast<T*>(str)), size_(len) {}
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

template <typename StrT, std::void_t<decltype(std::declval<StrT>().data())>* = nullptr>
inline span<char> giftStr(const StrT& s) noexcept {
	// Explicit const_cast for (s) to force COW-string copy
	return span<char>(const_cast<StrT&>(s).data(), s.size());
}
inline span<char> giftStr(std::string_view sv) noexcept { return span<char>(const_cast<char*>(sv.data()), sv.size()); }

}  // namespace reindexer
