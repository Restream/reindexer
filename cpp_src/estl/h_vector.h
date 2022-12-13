
#pragma once

#include <stdint.h>
#include <cstring>
#include <initializer_list>
#include <iostream>
#include <type_traits>
#include "tools/assertrx.h"
#include "trivial_reverse_iterator.h"

namespace reindexer {
#if 0
template <typename T, int holdSize>
class h_vector : public std::vector<T> {};
#else

#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ == 4
template <typename T>
using is_trivially_default_constructible = std::has_trivial_default_constructor<T>;
#else
template <typename T>
using is_trivially_default_constructible = std::is_trivially_default_constructible<T>;
#endif

template <typename T, int holdSize = 4, int objSize = sizeof(T)>
class h_vector {
	static_assert(holdSize > 0);

public:
	typedef T value_type;
	typedef T* pointer;
	typedef const T* const_pointer;
	typedef T& reference;
	typedef const T& const_reference;
	typedef const_pointer const_iterator;
	typedef pointer iterator;
	typedef trivial_reverse_iterator<const_iterator> const_reverse_iterator;
	typedef trivial_reverse_iterator<iterator> reverse_iterator;
	typedef unsigned size_type;
	typedef std::ptrdiff_t difference_type;
	h_vector() noexcept : e_{0, 0}, size_(0), is_hdata_(1) {}
	h_vector(size_type size) : h_vector() { resize(size); }
	h_vector(size_type size, const T& v) : h_vector() {
		reserve(size);
		const pointer p = ptr();
		for (size_type i = 0; i < size; ++i) new (p + i) T(v);
		size_ = size;
	}
	h_vector(std::initializer_list<T> l) : e_{0, 0}, size_(0), is_hdata_(1) { insert(begin(), l.begin(), l.end()); }
	template <typename InputIt>
	h_vector(InputIt first, InputIt last) : e_{0, 0}, size_(0), is_hdata_(1) {
		insert(begin(), first, last);
	}
	h_vector(const h_vector& other) : e_{0, 0}, size_(0), is_hdata_(1) {
		reserve(other.capacity());
		for (size_type i = 0; i < other.size(); i++) new (ptr() + i) T(other.ptr()[i]);
		size_ = other.size_;
	}
	h_vector(h_vector&& other) noexcept : size_(0), is_hdata_(1) {
		if (other.is_hdata()) {
			for (size_type i = 0; i < other.size(); i++) {
				new (ptr() + i) T(std::move(other.ptr()[i]));
				if (!std::is_trivially_destructible<T>::value) other.ptr()[i].~T();
			}
		} else {
			e_.data_ = other.e_.data_;
			e_.cap_ = other.capacity();
			other.is_hdata_ = 1;
			is_hdata_ = 0;
		}
		size_ = other.size_;
		other.size_ = 0;
	}
	~h_vector() { destruct(); }
	h_vector& operator=(const h_vector& other) {
		if (&other != this) {
			reserve(other.capacity());
			size_type mv = other.size() > size() ? size() : other.size();
			std::copy(other.begin(), other.begin() + mv, begin());
			size_type i = mv;
			for (; i < other.size(); i++) new (ptr() + i) T(other.ptr()[i]);
			if (!std::is_trivially_destructible<T>::value)
				for (; i < size(); i++) ptr()[i].~T();
			size_ = other.size_;
		}
		return *this;
	}

	h_vector& operator=(h_vector&& other) noexcept {
		if (&other != this) {
			clear();
			if (other.is_hdata()) {
				for (size_type i = 0; i < other.size(); i++) {
					new (ptr() + i) T(std::move(other.ptr()[i]));
					if constexpr (!std::is_trivially_destructible<T>::value) other.ptr()[i].~T();
				}
			} else {
				e_.data_ = other.e_.data_;
				e_.cap_ = other.capacity();
				other.is_hdata_ = 1;
				is_hdata_ = 0;
			}
			size_ = other.size_;
			other.size_ = 0;
		}
		return *this;
	}

	bool operator==(const h_vector& other) const noexcept {
		if (&other != this) {
			if (size() != other.size()) return false;
			for (size_t i = 0; i < size(); ++i) {
				if (!(at(i) == other.at(i))) return false;
			}
			return true;
		}
		return true;
	}
	bool operator!=(const h_vector& other) const noexcept { return !operator==(other); }

	template <bool FreeHeapMemory = true>
	void clear() noexcept {
		if constexpr (FreeHeapMemory) {
			destruct();
			is_hdata_ = 1;
		} else if constexpr (!std::is_trivially_destructible_v<T>) {
			auto p = ptr();
			for (size_type i = 0; i < size_; ++i) p[i].~T();
		}
		size_ = 0;
	}

	iterator begin() noexcept { return ptr(); }
	iterator end() noexcept { return ptr() + size_; }
	const_iterator begin() const noexcept { return ptr(); }
	const_iterator end() const noexcept { return ptr() + size_; }
	const_iterator cbegin() const noexcept { return ptr(); }
	const_iterator cend() const noexcept { return ptr() + size_; }
	const_reverse_iterator rbegin() const noexcept {
		const_reverse_iterator it;
		it = end();
		return it;
	}
	const_reverse_iterator rend() const noexcept {
		const_reverse_iterator it;
		it = begin();
		return it;
	}
	reverse_iterator rbegin() noexcept {
		reverse_iterator it;
		it = end();
		return it;
	}
	reverse_iterator rend() noexcept {
		reverse_iterator it;
		it = begin();
		return it;
	}
	size_type size() const noexcept { return size_; }
	size_type capacity() const noexcept { return is_hdata_ ? holdSize : e_.cap_; }
	bool empty() const noexcept { return size_ == 0; }
	const_reference operator[](size_type pos) const noexcept { return ptr()[pos]; }
	reference operator[](size_type pos) noexcept { return ptr()[pos]; }
	const_reference at(size_type pos) const {
		if (pos >= size()) {
			throw std::logic_error("h_vector: Out of range (pos: " + std::to_string(pos) + ", size: " + std::to_string(size()));
		}
		return ptr()[pos];
	}
	reference at(size_type pos) {
		if (pos >= size()) {
			throw std::logic_error("h_vector: Out of range (pos: " + std::to_string(pos) + ", size: " + std::to_string(size()));
		}
		return ptr()[pos];
	}
	reference back() noexcept { return ptr()[size() - 1]; }
	reference front() noexcept { return ptr()[0]; }
	const_reference back() const noexcept { return ptr()[size() - 1]; }
	const_reference front() const noexcept { return ptr()[0]; }
	const_pointer data() const noexcept { return ptr(); }
	pointer data() noexcept { return ptr(); }

	void resize(size_type sz) {
		grow(sz);
		if constexpr (!reindexer::is_trivially_default_constructible<T>::value) {
			for (size_type i = size_; i < sz; i++) new (ptr() + i) T();
		}
		if constexpr (!std::is_trivially_destructible<T>::value) {
			for (size_type i = sz; i < size_; i++) ptr()[i].~T();
		}
		size_ = sz;
	}
	void reserve(size_type sz) {
		if (sz > capacity()) {
			assertrx(sz > holdSize);
			// NOLINTNEXTLINE(bugprone-sizeof-expression)
			pointer new_data = static_cast<pointer>(operator new(sz * sizeof(T)));	// ?? dynamic
			pointer oold_data = ptr();
			pointer old_data = oold_data;
			for (size_type i = 0; i < size_; i++) {
				new (new_data + i) T(std::move(*old_data));
				if (!std::is_trivially_destructible<T>::value) old_data->~T();
				++old_data;
			}
			if (!is_hdata()) operator delete(oold_data);
			e_.data_ = new_data;
			e_.cap_ = sz;
			is_hdata_ = 0;
		}
	}
	void grow(size_type sz) {
		if (sz > capacity()) reserve(std::max(sz, capacity() * 2));
	}
	void push_back(const T& v) {
		grow(size_ + 1);
		new (ptr() + size_) T(v);
		size_++;
	}
	void push_back(T&& v) {
		grow(size_ + 1);
		new (ptr() + size_) T(std::move(v));
		size_++;
	}
	template <typename... Args>
	void emplace_back(Args&&... args) {
		grow(size_ + 1);
		new (ptr() + size_) T(std::forward<Args>(args)...);
		size_++;
	}
	void pop_back() {
		assertrx(size_);
		resize(size_ - 1);
	}
	iterator insert(const_iterator pos, const T& v) {
		const size_type i = pos - begin();
		if (i == size()) {
			push_back(v);
		} else {
			assertrx(i < size());
			grow(size_ + 1);
			const pointer p = ptr();
			new (p + size_) T(std::move(p[size_ - 1]));
			for (size_type j = size_ - 1; j > i; --j) {
				p[j] = std::move(p[j - 1]);
			}
			p[i] = v;
			++size_;
		}
		return begin() + i;
	}
	iterator insert(const_iterator pos, T&& v) {
		const size_type i = pos - begin();
		if (i == size()) {
			push_back(std::move(v));
		} else {
			assertrx(i < size());
			grow(size_ + 1);
			const pointer p = ptr();
			new (p + size_) T(std::move(p[size_ - 1]));
			for (size_type j = size_ - 1; j > i; --j) {
				p[j] = std::move(p[j - 1]);
			}
			p[i] = std::move(v);
			++size_;
		}
		return begin() + i;
	}
	iterator insert(const_iterator pos, difference_type count, const T& v) {
		if (count == 0) return const_cast<iterator>(pos);
		difference_type i = pos - begin();
		assertrx(i <= size());
		grow(size_ + count);
		const pointer p = ptr();
		difference_type j = size_ + count - 1;
		for (; j >= static_cast<difference_type>(size_) && j >= count + i; --j) {
			new (p + j) T(std::move(p[j - count]));
		}
		for (; j >= count + i; --j) {
			p[j] = std::move(p[j - count]);
		}
		for (; j >= size_; --j) {
			new (p + j) T(v);
		}
		for (; j >= i; --j) {
			p[j] = v;
		}
		size_ += count;
		return begin() + i;
	}
	template <typename... Args>
	iterator emplace(const_iterator pos, Args&&... args) {
		const size_type i = pos - begin();
		if (i == size()) {
			emplace_back(std::forward<Args>(args)...);
		} else {
			assertrx(i < size());
			grow(size_ + 1);
			const pointer p = ptr();
			new (p + size_) T(std::move(p[size_ - 1]));
			for (size_type j = size_ - 1; j > i; --j) {
				p[j] = std::move(p[j - 1]);
			}
			p[i] = {std::forward<Args>(args)...};
			++size_;
		}
		return begin() + i;
	}
	iterator erase(const_iterator it) { return erase(it, it + 1); }
	template <class InputIt>
	iterator insert(const_iterator pos, InputIt first, InputIt last) {
		assertrx(last >= first);
		const difference_type cnt = last - first;
		if (cnt == 0) return const_cast<iterator>(pos);
		difference_type i = pos - begin();
		assertrx(i <= size());
		grow(size_ + cnt);
		const pointer p = ptr();
		difference_type j = size_ + cnt - 1;
		for (; j >= static_cast<difference_type>(size_) && j >= cnt + i; --j) {
			new (p + j) T(std::move(p[j - cnt]));
		}
		for (; j >= cnt + i; --j) {
			p[j] = std::move(p[j - cnt]);
		}
		for (; j >= static_cast<difference_type>(size_); --j) {
			new (p + j) T(*--last);
		}
		for (; j >= i; --j) {
			p[j] = *--last;
		}
		size_ += cnt;
		return begin() + i;
	}
	template <class InputIt>
	void assign(InputIt first, InputIt last) {
		clear();
		insert(begin(), first, last);
	}
	iterator erase(const_iterator first, const_iterator last) {
		const size_type i = first - ptr();
		const auto cnt = last - first;
		assertrx(i <= size());

		if (cnt == 0) return begin() + i;
		std::move(begin() + i + cnt, end(), begin() + i);
		const auto newSize = size_ - cnt;
		if constexpr (!std::is_trivially_destructible<T>::value) {
			for (size_type i = newSize; i < size_; i++) ptr()[i].~T();
		}
		size_ = newSize;
		return begin() + i;
	}
	void shrink_to_fit() {
		if (is_hdata() || size_ == capacity()) return;

		h_vector tmp;
		tmp.reserve(size());
		tmp.insert(tmp.begin(), std::make_move_iterator(begin()), std::make_move_iterator(end()));
		*this = std::move(tmp);
	}
	size_t heap_size() const noexcept { return is_hdata_ ? 0 : e_.cap_ * sizeof(T); }
	bool is_hdata() const noexcept { return is_hdata_; }

protected:
	pointer ptr() noexcept { return is_hdata() ? reinterpret_cast<pointer>(hdata_) : e_.data_; }
	const_pointer ptr() const noexcept { return is_hdata() ? reinterpret_cast<const_pointer>(hdata_) : e_.data_; }
	void destruct() noexcept {
		if (is_hdata()) {
			if constexpr (!std::is_trivially_destructible_v<T>) {
				for (size_type i = 0; i < size_; ++i) reinterpret_cast<pointer>(hdata_)[i].~T();
			}
		} else {
			if constexpr (!std::is_trivially_destructible_v<T>) {
				for (size_type i = 0; i < size_; ++i) e_.data_[i].~T();
			}
			operator delete(e_.data_);
		}
	}

#pragma pack(push, 1)
	struct edata {
		pointer data_;
		size_type cap_;
	};
#pragma pack(pop)

	union {
		edata e_;
		uint8_t hdata_[holdSize > 0 ? holdSize* objSize : 1];
	};
	size_type size_ : 31;
	size_type is_hdata_ : 1;
};
#endif

template <typename C, int H>
inline static std::ostream& operator<<(std::ostream& o, const reindexer::h_vector<C, H>& vec) {
	o << "[";
	for (unsigned i = 0; i < vec.size(); i++) {
		if (i != 0) o << ",";
		o << vec[i] << " ";
	}
	o << "]";
	return o;
}

}  // namespace reindexer
