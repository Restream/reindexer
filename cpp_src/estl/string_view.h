#pragma once

#include <string.h>
#include <algorithm>
#include <iostream>
#include <string>
#include "tools/customhash.h"
/**
 * @namespace reindexer
 * The base namespace
 */
namespace reindexer {

/**
 * @class string_view
 * A character buffer wrapper.
 * Keeps a pointer to an original character with its size.
 */
class string_view {
public:
	typedef const char *iterator;

	/**
	 * Create an empty slice object.
	 */
	constexpr string_view() noexcept : ptr_(nullptr), size_(0) {}

	/**
	 * Create a slice object based on a pointer to character buffer with known size
	 * @param p a constant character buffer pointer
	 * @param sz the buffer size argument
	 */
	constexpr string_view(const char *p, size_t sz) noexcept : ptr_(p), size_(sz) {}

	/**
	 * Create a slice object based on a pointer to character buffer with its size computing
	 * @param p a constant character buffer pointer
	 */
	string_view(const char *p) noexcept : ptr_(p), size_(p ? strlen(p) : 0) {}

	/**
	 * Create a slice object based on string
	 * @param str a constant string reference
	 */
	string_view(const std::string &str) noexcept : ptr_(str.data()), size_(str.size()) {}

	/**
	 * @public
	 * Get a pointer to an original character buffer
	 * @return a constant character buffer pointer
	 */
	constexpr const char *data() const { return ptr_; }

	/**
	 * @public
	 * Get saved size of an original character buffer
	 */
	constexpr size_t size() const { return size_; }

	/**
	 * @public
	 * Get saved size of an original character buffer
	 */
	constexpr size_t length() const { return size_; }

	/**
	 * @public
	 * Check if orignal buffer is empty
	 */
	constexpr bool empty() const { return size_ == 0; }

	/**
	 * @public
	 * Get symbol at position
	 * @param idx index of position
	 */
	char operator[](size_t idx) const { return ptr_[idx]; }

	/**
	 * @public
	 * Get substring
	 * @param pos index of syaty position
	 * @param len length of substring
	 */
	string_view substr(size_t pos, size_t len = npos) const {
		pos = std::min(pos, size_);
		len = std::min(len, size_ - pos);
		return string_view(ptr_ + pos, len);
	}

	/**
	 * @public
	 * Find char
	 * @param sym symbol to find
	 * @param pos position since we start searching.
	 */
	size_t find(char sym, size_t pos = 0) const {
		for (; pos < size_; ++pos) {
			if (ptr_[pos] == sym) return pos;
		}
		return npos;
	}

	/**
	 * @public
	 * Find substring
	 * @param v string_view to find
	 * @param pos position since we start searching.
	 */
	size_t find(string_view v, size_t pos = 0) const {
		for (; pos + v.size() < size_; ++pos) {
			if (!memcmp(v.data(), data() + pos, v.size())) return pos;
		}
		return npos;
	}

	/**
	 * @public
	 * Searches for the first character that matches any of the characters specified in str parameter.
	 * @param str string of characters to find.
	 * @param pos position since we start searching.
	 */
	size_t find_first_of(const string_view &str, size_t pos = 0) const {
		for (; pos < size(); ++pos) {
			for (size_t i = 0; i < str.length(); ++i) {
				if (ptr_[pos] == str[i]) return pos;
			}
		}
		return npos;
	}

	/**
	 * @public
	 * Searches for the last character that matches any of the characters specified in str parameter.
	 * @param str string of characters to find.
	 * @param pos position since we start searching.
	 */
	size_t find_last_of(const string_view &str, size_t pos = npos) const {
		if (!size()) return npos;
		if (pos > size()) pos = size() - 1;

		do {
			for (size_t i = 0; i < str.length(); ++i) {
				if (ptr_[pos] == str[i]) return pos;
			}
		} while (pos--);
		return npos;
	}

	/**
	 * @public
	 * Compare slices
	 * @param other string_view to compare
	 */
	bool operator==(const string_view &other) const {
		if (other.size_ != size_) return false;
		return !memcmp(other.ptr_, ptr_, size_);
	}

	/**
	 * @public
	 * Compare slices
	 * @param other string_view to compare
	 */
	bool operator!=(const string_view &other) const { return !operator==(other); }

	/**
	 * @public
	 * Get an independent string based on an original character buffer with its size
	 * @return a string based on an original character buffer with its size
	 */
	explicit operator std::string() const { return std::string(ptr_, size_); }

	iterator begin() const { return ptr_; }
	iterator end() const { return ptr_ + size_; }

	static constexpr size_t npos = -1;

	// protected:
	/**
	 * @protected
	 * A constant character pointer to an original character buffer
	 */
	const char *ptr_;

	/**
	 * @protected
	 * A size of character buffer
	 */
	size_t size_;
};

constexpr string_view operator"" _sv(const char *str, size_t len) noexcept { return string_view(str, len); }

}  // namespace reindexer

#include <functional>

namespace std {
inline static std::ostream &operator<<(std::ostream &o, const reindexer::string_view &sv) {
	o.write(sv.data(), sv.length());
	return o;
}

template <>
struct hash<reindexer::string_view> {
public:
	size_t operator()(reindexer::string_view s) const { return reindexer::_Hash_bytes(s.data(), s.length()); }
};

}  // namespace std
