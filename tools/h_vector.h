
#pragma once

#include <assert.h>
#include <cstring>
#include <initializer_list>
#include <vector>
namespace reindexer {
#if 0
template <typename T, int holdSize>
class h_vector : public std::vector<T> {};
#else
template <typename T, int holdSize>
class h_vector {
public:
	typedef T value_type;
	typedef T* pointer;
	typedef const T* const_pointer;
	typedef T& reference;
	typedef const T& const_reference;
	typedef const_pointer const_iterator;
	typedef pointer iterator;
	typedef std::reverse_iterator<const_iterator> const_reverse_iterator;
	typedef std::reverse_iterator<iterator> reverse_iterator;
	typedef unsigned size_type;
	typedef std::ptrdiff_t difference_type;
	h_vector() noexcept : size_(0), cap_(holdSize) {}
	//	h_vector(std::initializer_list<T> l) : size_(0), cap_(holdSize) { insert(begin(), l.begin(), l.end()); }
	h_vector(const h_vector& other) : size_(0), cap_(holdSize) {
		reserve(other.size());
		for (size_type i = 0; i < other.size(); i++) new (ptr() + i) T(other.ptr()[i]);
		size_ = other.size_;
	}
	h_vector(h_vector&& other) noexcept : size_(0), cap_(holdSize) {
		if (other.is_hdata()) {
			for (size_type i = 0; i < other.size(); i++) new (ptr() + i) T(std::move(other.ptr()[i]));
		} else {
			data_ = other.data_;
			cap_ = other.capacity();
			other.cap_ = holdSize;
		}
		size_ = other.size_;
		other.size_ = 0;
	}
	~h_vector() {
		assert(cap_ >= holdSize);
		clear();
	}
	h_vector& operator=(const h_vector& other) {
		if (&other != this) {
			reserve(other.size());
			size_type mv = other.size() > size() ? size() : other.size();
			std::copy(other.begin(), other.begin() + mv, begin());
			size_type i = mv;
			for (; i < other.size(); i++) new (ptr() + i) T(other.ptr()[i]);
			for (; i < size(); i++) ptr()[i].~T();
			size_ = other.size_;
		}
		return *this;
	}

	h_vector& operator=(h_vector&& other) noexcept {
		if (&other != this) {
			if (other.is_hdata()) {
				size_type mv = other.size() > size() ? size() : other.size();
				std::move(other.begin(), other.begin() + mv, begin());
				size_type i = mv;
				for (; i < other.size(); i++) new (ptr() + i) T(std::move(other.ptr()[i]));
				for (; i < size(); i++) ptr()[i].~T();
			} else {
				clear();
				data_ = other.data_;
				cap_ = other.capacity();
				other.cap_ = holdSize;
			}
			size_ = other.size_;
			other.size_ = 0;
		}
		return *this;
	}
	void clear() {
		resize(0);
		if (!is_hdata()) operator delete(static_cast<void*>(data_));
		cap_ = holdSize;
	}

	iterator begin() noexcept { return ptr(); }
	iterator end() noexcept { return ptr() + size_; }
	const_iterator begin() const noexcept { return ptr(); }
	const_iterator end() const noexcept { return ptr() + size_; }
	reverse_iterator rbegin() const noexcept { return reverse_iterator(end()); }
	reverse_iterator rend() const noexcept { return reverse_iterator(begin()); }
	size_type size() const noexcept { return size_; }
	size_type capacity() const noexcept { return cap_; }
	bool empty() const noexcept { return !!size_; }
	const T& operator[](size_type pos) const { return ptr()[pos]; }
	T& operator[](size_type pos) { return ptr()[pos]; }
	const T& at(size_type pos) const { return ptr()[pos]; }
	T& at(size_type pos) { return ptr()[pos]; }
	T& back() { return ptr()[size() - 1]; }
	const T* data() const noexcept { return ptr(); }
	void resize(size_type sz) {
		grow(sz);
		for (size_type i = size_; i < sz; i++) new (ptr() + i) T();
		for (size_type i = sz; i < size_; i++) ptr()[i].~T();
		size_ = sz;
	}
	void reserve(size_type sz) {
		if (sz > capacity()) {
			assert(cap_ >= holdSize);
			pointer new_data = static_cast<pointer>(operator new(sz * sizeof(T)));  // ?? dynamic
			pointer oold_data = ptr();
			pointer old_data = ptr();
			for (size_type i = 0; i < size_; i++) {
				new (new_data + i) T(std::move(*old_data));
				old_data->~T();
				old_data++;
			}
			if (!is_hdata()) operator delete(static_cast<void*>(oold_data));
			data_ = new_data;
			cap_ = sz;
		}
	}
	void grow(size_type sz) {
		if (sz > capacity()) reserve(sz + capacity() * 2);
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
	void pop_back() {
		assert(size_);
		resize(size_ - 1);
	}
	iterator insert(const_iterator pos, const T& v) {
		size_type i = pos - begin();
		assert(i <= size());
		grow(size_ + 1);
		resize(size_ + 1);
		std::move_backward(begin() + i, end() - 1, end());
		ptr()[i] = v;
		return begin() + i;
	}
	iterator insert(const_iterator pos, T&& v) {
		size_type i = pos - begin();
		assert(i <= size());
		grow(size_ + 1);
		resize(size_ + 1);
		std::move_backward(begin() + i, end() - 1, end());
		ptr()[i] = std::move(v);
		return begin() + i;
	}
	iterator erase(const_iterator it) { return erase(it, it + 1); }
	template <class InputIt>
	iterator insert(const_iterator pos, InputIt first, InputIt last) {
		size_type i = pos - begin();
		assert(i <= size());
		auto cnt = last - first;
		grow(size_ + cnt);
		resize(size_ + cnt);
		std::move_backward(begin() + i, end() - cnt, end());
		std::copy(first, last, begin() + i);
		return begin() + i;
	}
	iterator erase(const_iterator first, const_iterator last) {
		size_type i = first - ptr();
		auto cnt = last - first;
		assert(i <= size());

		std::move(begin() + i + cnt, end(), begin() + i);
		resize(size_ - (last - first));
		return begin() + i;
	}
	void shrink_to_fit() {
		if (is_hdata() || size_ == capacity()) return;

		h_vector tmp(static_cast<const h_vector&>(*this));  // ?? dynamic
		clear();
		*this = std::move(tmp);
	}

protected:
	bool is_hdata() const noexcept { return cap_ == holdSize; }
	pointer ptr() noexcept { return is_hdata() ? reinterpret_cast<pointer>(hdata_) : data_; }
	const_pointer ptr() const noexcept { return is_hdata() ? reinterpret_cast<const_pointer>(hdata_) : data_; }

	size_type size_;
	size_type cap_;
	union {
		pointer data_;
		uint8_t hdata_[holdSize * sizeof(T)];
	};
};
#endif

template <typename T>
class h_vector_view {
public:
	typedef T value_type;
	typedef T* pointer;
	typedef const T* const_pointer;
	typedef const_pointer const_iterator;
	typedef pointer iterator;
	typedef std::reverse_iterator<const_iterator> const_reverse_iterator;
	typedef std::reverse_iterator<iterator> reverse_iterator;
	typedef size_t size_type;

	constexpr h_vector_view() noexcept : data_(nullptr), size_(0) {}
	constexpr h_vector_view(const h_vector_view& other) noexcept : data_(other.data_), size_(other.size_) {}

	h_vector_view& operator=(const h_vector_view& other) noexcept {
		data_ = other.data_;
		size_ = other.size_;
		return *this;
	}

	h_vector_view& operator=(h_vector_view&& other) noexcept {
		data_ = other.data_;
		size_ = other.size_;
		return *this;
	}

	// FIXME: const override
	constexpr h_vector_view(const T* str, size_type len) : data_(const_cast<T*>(str)), size_(len) {}  //static??
	constexpr iterator begin() const noexcept { return data_; }
	constexpr iterator end() const noexcept { return data_ + size_; }
	constexpr reverse_iterator rbegin() const noexcept { return reverse_iterator(end()); }
	constexpr reverse_iterator rend() const noexcept { return reverse_iterator(begin()); }
	constexpr size_type size() const noexcept { return size_; }
	constexpr bool empty() const noexcept { return !!size_; }
	constexpr const T& operator[](size_type pos) const { return data_[pos]; }
	T& operator[](size_type pos) { return data_[pos]; }
	constexpr const T& at(size_type pos) const { return data_[pos]; }
	constexpr const T& front() const { return data_[0]; }
	constexpr const T& back() const { return data_[size() - 1]; }
	constexpr const T* data() const noexcept { return data_; }

protected:
	pointer data_;
	size_type size_;
};

}  // namespace reindexer
