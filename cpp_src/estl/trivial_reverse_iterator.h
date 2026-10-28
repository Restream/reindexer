#pragma once
#include <iterator>

namespace reindexer {
using std::iterator_traits;

template <class Iterator>
class [[nodiscard]] trivial_reverse_iterator {
public:
	typedef trivial_reverse_iterator this_type;
	typedef Iterator iterator_type;
	typedef typename iterator_traits<Iterator>::iterator_category iterator_category;
	typedef typename iterator_traits<Iterator>::value_type value_type;
	typedef typename iterator_traits<Iterator>::difference_type difference_type;
	typedef typename iterator_traits<Iterator>::reference reference;
	typedef typename iterator_traits<Iterator>::pointer pointer;

public:
	constexpr trivial_reverse_iterator() = default;
	constexpr trivial_reverse_iterator(Iterator it) noexcept : current_(it) {
		static_assert(std::is_trivial_v<this_type>, "Expecting std::is_trivial_v");
	}

	template <class Up>
	trivial_reverse_iterator& operator=(const trivial_reverse_iterator<Up>& u) noexcept {
		current_ = u.base();
		return *this;
	}

	Iterator base() const noexcept { return current_; }
	reference operator*() const noexcept {
		Iterator tmp = current_;
		return *--tmp;
	}
	pointer operator->() const noexcept { return std::addressof(operator*()); }
	trivial_reverse_iterator& operator++() noexcept {
		--current_;
		return *this;
	}
	trivial_reverse_iterator operator++(int) noexcept {
		trivial_reverse_iterator tmp(*this);
		--current_;
		return tmp;
	}
	trivial_reverse_iterator& operator--() noexcept {
		++current_;
		return *this;
	}
	trivial_reverse_iterator operator--(int) noexcept {
		trivial_reverse_iterator tmp(*this);
		++current_;
		return tmp;
	}
	trivial_reverse_iterator operator+(difference_type n) const noexcept { return current_ - n; }
	trivial_reverse_iterator& operator+=(difference_type n) noexcept {
		current_ -= n;
		return *this;
	}
	trivial_reverse_iterator operator-(difference_type n) const noexcept { return current_ + n; }
	trivial_reverse_iterator& operator-=(difference_type n) noexcept {
		current_ += n;
		return *this;
	}
	reference operator[](difference_type n) const noexcept { return *(*this + n); }

	// Assign operator overloading from const std::reverse_iterator<U>
	template <typename U>
	trivial_reverse_iterator& operator=(const std::reverse_iterator<U>& u) noexcept {
		if (current_ != u.base()) {
			current_ = u.base();
		}
		return *this;
	}

	// Assign operator overloading from non-const std::reverse_iterator<U>
	template <typename U>
	trivial_reverse_iterator& operator=(std::reverse_iterator<U>& u) noexcept {
		if (current_ != u.base()) {
			current_ = u.base();
		}
		return *this;
	}

	// Assign native pointer
	template <class Upn>
	trivial_reverse_iterator& operator=(Upn ptr) noexcept {
		static_assert(std::is_pointer<Upn>::value, "attempting assign a non-trivial pointer");
		/*if (current_ != ptr)*/ current_ = ptr;
		return *this;
	}

	inline bool operator!=(const this_type& rhs) const noexcept { return !EQ(current_, rhs.current_); }
	inline bool operator==(const this_type& rhs) const noexcept { return EQ(current_, rhs.current_); }

protected:
	Iterator current_;

private:
	inline bool EQ(Iterator lhs, Iterator rhs) const noexcept { return lhs == rhs; }
};

}  // namespace reindexer
