#pragma once
#include <iterator>

namespace reindexer {
using std::iterator;
using std::iterator_traits;

template <class Iterator>
class trivial_reverse_iterator
	: public iterator<typename iterator_traits<Iterator>::iterator_category, typename iterator_traits<Iterator>::value_type,
					  typename iterator_traits<Iterator>::difference_type, typename iterator_traits<Iterator>::pointer,
					  typename iterator_traits<Iterator>::reference> {
public:
	typedef trivial_reverse_iterator this_type;
	typedef Iterator iterator_type;
	typedef typename iterator_traits<Iterator>::difference_type difference_type;
	typedef typename iterator_traits<Iterator>::reference reference;
	typedef typename iterator_traits<Iterator>::pointer pointer;

public:
	//	if CTOR is enabled std::is_trivial<trvial_reverse_iterator<...>> return false;
	//	trivial_reverse_iterator() : current_(nullptr) {}

	template <class Up>
	trivial_reverse_iterator& operator=(const trivial_reverse_iterator<Up>& u) {
		current_ = u.base();
		return *this;
	}

	Iterator base() const { return current_; }
	reference operator*() const {
		Iterator tmp = current_;
		return *--tmp;
	}
	pointer operator->() const { return std::addressof(operator*()); }
	trivial_reverse_iterator& operator++() {
		--current_;
		return *this;
	}
	trivial_reverse_iterator operator++(int) {
		trivial_reverse_iterator tmp(*this);
		--current_;
		return tmp;
	}
	trivial_reverse_iterator& operator--() {
		++current_;
		return *this;
	}
	trivial_reverse_iterator operator--(int) {
		trivial_reverse_iterator tmp(*this);
		++current_;
		return tmp;
	}
	trivial_reverse_iterator operator+(difference_type n) const {
		Iterator ptr = current_ - n;
		trivial_reverse_iterator tmp;
		tmp = ptr;
		return tmp;
	}
	trivial_reverse_iterator& operator+=(difference_type n) {
		current_ -= n;
		return *this;
	}
	trivial_reverse_iterator operator-(difference_type n) const {
		Iterator ptr = current_ + n;
		trivial_reverse_iterator tmp;
		tmp = ptr;
		return tmp;
	}
	trivial_reverse_iterator& operator-=(difference_type n) {
		current_ += n;
		return *this;
	}
	reference operator[](difference_type n) const { return *(*this + n); }

	// Assign operator overloading from const std::reverse_iterator<U>
	template <typename U>
	trivial_reverse_iterator& operator=(const std::reverse_iterator<U>& u) {
		if (current_ != u.base()) current_ = u.base();
		return *this;
	}

	// Assign operator overloading from non-const std::reverse_iterator<U>
	template <typename U>
	trivial_reverse_iterator& operator=(std::reverse_iterator<U>& u) {
		if (current_ != u.base()) current_ = u.base();
		return *this;
	}

	// Assign native pointer
	template <class Upn>
	trivial_reverse_iterator& operator=(Upn ptr) {
		static_assert(std::is_pointer<Upn>::value, "attempting assign a non-trivial pointer");
		/*if (current_ != ptr)*/ current_ = ptr;
		return *this;
	}

	inline bool operator!=(const this_type& rhs) const { return !EQ(current_, rhs.current_); }
	inline bool operator==(const this_type& rhs) const { return EQ(current_, rhs.current_); }

protected:
	Iterator current_;

private:
	inline bool EQ(Iterator lhs, Iterator rhs) const { return lhs == rhs; }
};

}  // namespace reindexer
