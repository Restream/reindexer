#pragma once

#include <cstring>
#include <iostream>
#include <limits>

#ifdef REINDEX_DEBUG_CONTAINERS
#include <vector>
#else  // !REINDEX_DEBUG_CONTAINERS
#include <cstdint>
#include <initializer_list>
#include <string>
#include <type_traits>
#include <utility>
#include "debug_macros.h"
#include "estl/defines.h"
#include "tools/assertrx.h"
#include "trivial_reverse_iterator.h"
#endif	// !REINDEX_DEBUG_CONTAINERS

namespace reindexer {
#ifdef REINDEX_DEBUG_CONTAINERS

template <typename T, int holdSize = 4, unsigned objSize = sizeof(T)>
class [[nodiscard]] h_vector : public std::vector<T> {
public:
	typedef unsigned size_type;

	using std::vector<T>::vector;

	[[nodiscard]] bool operator==(const h_vector& other) const = default;

	template <bool F = true>
	void clear() noexcept {
		std::vector<T>::clear();
	}

	[[nodiscard]] size_t heap_size() const noexcept { return std::vector<T>::capacity() * sizeof(T); }

	[[nodiscard]] bool is_hdata() const noexcept { return false; }
	[[nodiscard]] static constexpr size_type max_size() noexcept { return std::numeric_limits<size_type>::max() >> 1; }

	void grow(size_type sz) { std::vector<T>::reserve(sz); }
};
#else  // !REINDEX_DEBUG_CONTAINERS

template <typename T, unsigned holdSize = 4, unsigned objSize = sizeof(T)>
class [[nodiscard]] h_vector {
	static_assert(holdSize > 0);

	class [[nodiscard]] StolenHeap {
	public:
		StolenHeap() noexcept = default;
		StolenHeap(T* ptr, size_t cap) noexcept : ptr_{ptr}, capacity_{cap} {}
		StolenHeap(StolenHeap&& other) noexcept : ptr_{std::exchange(other.ptr_, nullptr)}, capacity_{std::exchange(other.capacity_, 0)} {}
		~StolenHeap() { operator delete(ptr_); }

		[[nodiscard]] size_t Capacity() const noexcept { return capacity_; }
		[[nodiscard]] T* Release() && noexcept {
			capacity_ = 0;
			return std::exchange(ptr_, nullptr);
		}

		StolenHeap(const StolenHeap&) = delete;
		StolenHeap& operator=(const StolenHeap&) = delete;
		StolenHeap& operator=(StolenHeap&&) = delete;

	private:
		T* ptr_{nullptr};
		size_t capacity_{0};
	};

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
	static constexpr auto kElemSize = objSize;
	static constexpr auto kHoldSize = holdSize;
	static_assert(std::is_trivial_v<reverse_iterator>, "Expecting trivial reverse iterator");
	static_assert(std::is_trivial_v<const_reverse_iterator>, "Expecting trivial const reverse iterator");

	h_vector() noexcept : e_{0, 0}, size_(0), is_hdata_(1) {}
	h_vector(StolenHeap&& heap) noexcept : h_vector() {
		const auto cap = heap.Capacity();
		if (cap > holdSize) {
			e_.data_ = std::move(heap).Release();
			e_.cap_ = cap;
			is_hdata_ = 0;
		}
	}
	explicit h_vector(size_type size) : h_vector() { resize(size); }
	h_vector(size_type size, const T& v) : h_vector() {
		reserve(size);
		const iterator p = ptr();
		std::uninitialized_fill(p, p + size, v);
		size_ = size;
	}
	h_vector(std::initializer_list<T> l) : e_{0, 0}, size_(0), is_hdata_(1) {
		insert(begin(), std::make_move_iterator(l.begin()), std::make_move_iterator(l.end()));
	}
	template <typename InputIt>
	h_vector(InputIt first, InputIt last) : e_{0, 0}, size_(0), is_hdata_(1) {
		insert(begin(), first, last);
	}
	template <typename InputIt>
	h_vector(StolenHeap&& heap, InputIt first, InputIt last) : h_vector() {
		const auto cap = heap.Capacity();
		if (cap > holdSize && cap > (last - first)) {
			e_.data_ = std::move(heap).Release();
			e_.cap_ = cap;
			is_hdata_ = 0;
		}
		insert(begin(), first, last);
	}
	h_vector(const h_vector& other) : e_{0, 0}, size_(0), is_hdata_(1) {
		reserve(other.capacity());
		const pointer p = ptr();
		const_pointer op = other.ptr();
		const size_type osz = other.size();
		for (size_type i = 0; i < osz; i++) {
			new (p + i) T(op[i]);
		}
		size_ = other.size_;
	}
	h_vector(h_vector&& other) noexcept : e_{0, 0}, size_(0), is_hdata_(1) {
		if (other.is_hdata()) {
			const pointer p = reinterpret_cast<pointer>(hdata_);
			const pointer op = reinterpret_cast<pointer>(other.hdata_);
			const size_type osz = other.size();
			for (size_type i = 0; i < osz; i++) {
				new (p + i) T(std::move(op[i]));
				if constexpr (!std::is_trivially_destructible_v<T>) {
					op[i].~T();
				}
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
			const pointer p = ptr();
			const_pointer op = other.ptr();
			const auto osz = other.size();
			for (; i < osz; i++) {
				new (p + i) T(op[i]);
			}
			if constexpr (!std::is_trivially_destructible_v<T>) {
				const auto old_sz = size();
				for (; i < old_sz; i++) {
					p[i].~T();
				}
			}
			size_ = other.size_;
		}
		return *this;
	}

	h_vector& operator=(h_vector&& other) noexcept {
		if (&other != this) {
			clear();
			if (other.is_hdata()) {
				const size_type osz = other.size();
				const pointer p = ptr();
				const pointer op = other.ptr();
				for (size_type i = 0; i < osz; i++) {
					new (p + i) T(std::move(op[i]));
					if constexpr (!std::is_trivially_destructible_v<T>) {
						op[i].~T();
					}
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

	bool operator==(const h_vector& other) const noexcept(noexcept(std::declval<value_type>() == std::declval<value_type>())) {
		if (&other != this) {
			const size_type sz = size_;
			if (sz != other.size()) {
				return false;
			}
			for (size_t i = 0; i < sz; ++i) {
				if (!(operator[](i) == other[i])) {
					return false;
				}
			}
			return true;
		}
		return true;
	}
	bool operator!=(const h_vector& other) const noexcept(noexcept(std::declval<h_vector>() == std::declval<h_vector>())) {
		return !operator==(other);
	}

	static constexpr size_type max_size() noexcept { return std::numeric_limits<size_type>::max() >> 1; }

	template <bool FreeHeapMemory = true>
	void clear() noexcept {
		if constexpr (FreeHeapMemory) {
			destruct();
			is_hdata_ = 1;
		} else if constexpr (!std::is_trivially_destructible_v<T>) {
			const pointer p = ptr();
			const size_type sz = size_;
			for (size_type i = 0; i < sz; ++i) {
				p[i].~T();
			}
		}
		size_ = 0;
	}

	iterator begin() noexcept { return ptr(); }
	iterator end() noexcept { return ptr() + size_; }
	const_iterator begin() const noexcept { return ptr(); }
	const_iterator end() const noexcept { return ptr() + size_; }
	const_iterator cbegin() const noexcept { return ptr(); }
	const_iterator cend() const noexcept { return ptr() + size_; }
	const_reverse_iterator rbegin() const noexcept { return end(); }
	const_reverse_iterator rend() const noexcept { return begin(); }
	reverse_iterator rbegin() noexcept { return end(); }
	reverse_iterator rend() noexcept { return begin(); }
	size_type size() const noexcept { return size_; }
	size_type capacity() const noexcept { return is_hdata_ ? holdSize : e_.cap_; }
	bool empty() const noexcept { return size_ == 0; }
	const_reference operator[](size_type pos) const noexcept {
		rx_debug_check_subscript(pos);
		return ptr()[pos];
	}
	reference operator[](size_type pos) noexcept {
		rx_debug_check_subscript(pos);
		return ptr()[pos];
	}
	const_reference at(size_type pos) const {
		if (pos >= size()) [[unlikely]] {
			throw std::logic_error("h_vector: Out of range (pos: " + std::to_string(pos) + ", size: " + std::to_string(size()));
		}
		return ptr()[pos];
	}
	reference at(size_type pos) {
		if (pos >= size()) [[unlikely]] {
			throw std::logic_error("h_vector: Out of range (pos: " + std::to_string(pos) + ", size: " + std::to_string(size()));
		}
		return ptr()[pos];
	}
	reference back() noexcept {
		rx_debug_check_nonempty();
		return ptr()[size() - 1];
	}
	reference front() noexcept {
		rx_debug_check_nonempty();
		return ptr()[0];
	}
	const_reference back() const noexcept {
		rx_debug_check_nonempty();
		return ptr()[size() - 1];
	}
	const_reference front() const noexcept {
		rx_debug_check_nonempty();
		return ptr()[0];
	}
	const_pointer data() const noexcept { return ptr(); }
	pointer data() noexcept { return ptr(); }

	void resize(size_type sz) {
		grow(sz);
		if constexpr (!std::is_trivially_default_constructible<T>::value) {
			const pointer p = ptr();
			const size_type old_sz = size_;
			for (size_type i = old_sz; i < sz; ++i) {
				new (p + i) T();
			}
		}
		if constexpr (!std::is_trivially_destructible_v<T>) {
			const pointer p = ptr();
			const size_type old_sz = size_;
			for (size_type i = sz; i < old_sz; ++i) {
				p[i].~T();
			}
		}
		size_ = sz;
	}
	void resize(size_type sz, const T& default_value) {
		grow(sz);
		const size_type old_sz = size_;
		const pointer p = ptr();
		for (size_type i = old_sz; i < sz; ++i) {
			new (p + i) T(default_value);
		}
		if constexpr (!std::is_trivially_destructible_v<T>) {
			for (size_type i = sz; i < old_sz; ++i) {
				p[i].~T();
			}
		}
		size_ = sz;
	}
	void reserve(size_type sz) {
		if (sz > capacity()) {
			if (sz > max_size()) [[unlikely]] {
				throw std::logic_error("h_vector: max capacity overflow (requested: " + std::to_string(sz) +
									   ", max_size: " + std::to_string(max_size()) + " )");
			}
			if (sz <= holdSize) [[unlikely]] {
				throw std::logic_error("h_vector: unexpected reserved size");
			}
			// NOLINTNEXTLINE(bugprone-sizeof-expression)
			pointer new_data = static_cast<pointer>(operator new(sz * sizeof(T)));	// ?? dynamic
			pointer oold_data = ptr();
			pointer old_data = oold_data;
			// Creating those explicit old_sz variable for better vectorization
			for (size_type i = 0, old_sz = size_; i < old_sz; ++i) {
				new (new_data + i) T(std::move(*old_data));
				if constexpr (!std::is_trivially_destructible_v<T>) {
					old_data->~T();
				}
				++old_data;
			}
			if (!is_hdata()) {
				operator delete(oold_data);
			}
			e_.data_ = new_data;
			e_.cap_ = sz;
			is_hdata_ = 0;
		}
	}
	void grow(size_type sz) {
		const auto cap = capacity();
		if (sz > cap) {
			reserve(std::max(sz, std::min(max_size(), cap * 2)));
		}
	}
	void push_back(const T& v) {
		const auto size = size_;
		grow(size + 1);
		new (ptr() + size) T(v);
		++size_;
	}
	void push_back(T&& v) {
		const auto size = size_;
		grow(size + 1);
		new (ptr() + size) T(std::move(v));
		++size_;
	}
	template <typename... Args>
	reference emplace_back(Args&&... args) {
		const auto size = size_;
		grow(size + 1);
		auto p = ptr() + size;
		new (p) T(std::forward<Args>(args)...);
		++size_;
		return *p;
	}
	void pop_back() {
		rx_debug_check_nonempty();
		if constexpr (!std::is_trivially_destructible_v<T>) {
			ptr()[--size_].~T();
		} else {
			--size_;
		}
	}
	iterator insert(const_iterator pos, const T& v) {
		const size_type i = pos - begin();
		if (i == size()) {
			push_back(v);
		} else {
			rx_debug_check_subscript(i);
			const size_type sz = size_;
			grow(sz + 1);
			const pointer p = ptr();
			new (p + sz) T(std::move(p[sz - 1]));
			for (size_type j = sz - 1; j > i; --j) {
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
			rx_debug_check_subscript(i);
			const size_type sz = size_;
			grow(sz + 1);
			const pointer p = ptr();
			new (p + sz) T(std::move(p[sz - 1]));
			for (size_type j = sz - 1; j > i; --j) {
				p[j] = std::move(p[j - 1]);
			}
			p[i] = std::move(v);
			++size_;
		}
		return begin() + i;
	}
	iterator insert(const_iterator pos, difference_type count, const T& v) {
		if (count == 0) {
			return const_cast<iterator>(pos);
		}
		difference_type i = pos - begin();
		rx_debug_check_subscript_le(i);
		const int64_t sz = size_;
		grow(sz + count);
		const pointer p = ptr();
		difference_type j = sz + count - 1;
		for (; j >= sz && j >= count + i; --j) {
			new (p + j) T(std::move(p[j - count]));
		}
		for (; j >= count + i; --j) {
			p[j] = std::move(p[j - count]);
		}
		for (; j >= sz; --j) {
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
		const size_type sz = size_;
		if (i == sz) {
			emplace_back(std::forward<Args>(args)...);
		} else {
			rx_debug_check_subscript(i);
			grow(sz + 1);
			const pointer p = ptr();
			new (p + sz) T(std::move(p[sz - 1]));
			for (size_type j = sz - 1; j > i; --j) {
				p[j] = std::move(p[j - 1]);
			}
			p[i] = T(std::forward<Args>(args)...);
			++size_;
		}
		return begin() + i;
	}
	iterator erase(const_iterator it) {
		pointer p = ptr();
		const size_type i = it - p;
		rx_debug_check_subscript(i);

		auto firstPtr = p + i;
		std::move(firstPtr + 1, p + size_, firstPtr);
		--size_;
		if constexpr (!std::is_trivially_destructible_v<T>) {
			p[size_].~T();
		}
		return firstPtr;
	}
	template <class InputIt>
	iterator insert(const_iterator pos, InputIt first, InputIt last) {
		rx_debug_check_valid_range(first, last);
		const difference_type cnt = last - first;
		if (cnt == 0) {
			return const_cast<iterator>(pos);
		}
		const difference_type i = pos - begin();
		rx_debug_check_subscript_le(i);
		const int64_t sz = size_;
		grow(sz + cnt);
		const pointer p = ptr();
		difference_type j = sz + cnt - 1;
		for (; j >= sz && j >= cnt + i; --j) {
			new (p + j) T(std::move(p[j - cnt]));
		}
		for (; j >= cnt + i; --j) {
			p[j] = std::move(p[j - cnt]);
		}
		for (; j >= sz; --j) {
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
		static_assert(std::is_same_v<typename std::iterator_traits<InputIt>::iterator_category, std::random_access_iterator_tag>,
					  "Expecting random access iterators here");
		rx_debug_check_valid_range(first, last);
		const int64_t cnt = std::distance(first, last);
		const int64_t cap = capacity();
		if (cap >= cnt && cap - (cnt >> 2) <= cnt) {
			// Allow up to 25% extra memory
			clear<false>();
		} else {
			clear();
			grow(cnt);
		}
		std::uninitialized_copy(first, last, begin());
		size_ = cnt;
	}
	iterator erase(const_iterator first, const_iterator last) {
		rx_debug_check_valid_range(first, last);
		pointer p = ptr();
		const size_type i = first - p;
		const auto cnt = last - first;
		auto firstPtr = p + i;
		if (cnt == 0) {
			rx_debug_check_subscript_le(i);
			return firstPtr;
		}
		rx_debug_check_subscript(i);
		const size_type sz = size_;

		std::move(std::make_move_iterator(firstPtr + cnt), std::make_move_iterator(p + sz), firstPtr);
		const auto newSize = sz - cnt;
		if constexpr (!std::is_trivially_destructible_v<T>) {
			for (size_type j = newSize; j < sz; ++j) {
				p[j].~T();
			}
		}
		size_ = newSize;
		return firstPtr;
	}
	void shrink_to_fit() {
		const auto sz = size();
		if (is_hdata() || sz == capacity()) {
			return;
		}

		h_vector tmp;
		tmp.reserve(sz);
		std::move(std::make_move_iterator(begin()), std::make_move_iterator(end()), tmp.begin());
		tmp.size_ = sz;
		*this = std::move(tmp);
	}
	size_t heap_size() const noexcept { return is_hdata_ ? 0 : e_.cap_ * sizeof(T); }
	bool is_hdata() const noexcept { return is_hdata_; }
	StolenHeap steal_heap() && noexcept {
		if (is_hdata()) {
			return {};
		}
		clear<false>();
		StolenHeap res{e_.data_, capacity()};
		is_hdata_ = true;
		return res;
	}
	// This implementation also transfers capacity values
	void swap(h_vector& other) noexcept {
		[[maybe_unused]] auto cap1 = capacity();
		[[maybe_unused]] auto cap2 = other.capacity();
		std::swap(*this, other);
		assertrx(capacity() == cap2);
		assertrx(other.capacity() == cap1);
	}

protected:
	pointer ptr() noexcept { return is_hdata() ? reinterpret_cast<pointer>(hdata_) : e_.data_; }
	const_pointer ptr() const noexcept { return is_hdata() ? reinterpret_cast<const_pointer>(hdata_) : e_.data_; }
	RX_ALWAYS_INLINE void destruct() noexcept {
		if (is_hdata()) {
			if constexpr (!std::is_trivially_destructible_v<T>) {
				auto beg = reinterpret_cast<pointer>(hdata_), end = beg + size_;
				for (auto ptr = beg; ptr != end; ++ptr) {
					ptr->~T();
				}
			}
		} else {
			if constexpr (!std::is_trivially_destructible_v<T>) {
				auto beg = e_.data_, end = beg + size_;
				for (auto ptr = beg; ptr != end; ++ptr) {
					ptr->~T();
				}
			}
			operator delete(e_.data_);
		}
	}

#pragma pack(push, 1)
	struct [[nodiscard]] edata {
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
#endif	// !REINDEX_DEBUG_CONTAINERS

template <typename C, unsigned H>
inline std::ostream& operator<<(std::ostream& o, const reindexer::h_vector<C, H>& vec) {
	o << "[";
	for (unsigned i = 0; i < vec.size(); i++) {
		if (i != 0) {
			o << ",";
		}
		o << vec[i] << " ";
	}
	o << "]";
	return o;
}

}  // namespace reindexer
