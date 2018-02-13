#pragma once

#include <string.h>
#include <string>

/**
 * @namespace reindexer
 * The base namespace
 */
namespace reindexer {

using std::string;

/**
 * @class Slice
 * A character buffer wrapper.
 * Keeps a pointer to an original character with its size.
 */
struct Slice {
	/**
	 * Create an empty slice object.
	 */
	Slice() : ptr_(nullptr), size_(0) {}

	/**
	 * Create a slice object based on a pointer to character buffer with known size
	 * @param p a constant character buffer pointer
	 * @param sz the buffer size argument
	 */
	Slice(const char *p, size_t sz) : ptr_(p), size_(sz) {}

	/**
	 * Create a slice object based on a pointer to character buffer with its size computing
	 * @param p a constant character buffer pointer
	 */
	Slice(const char *p) : ptr_(p), size_(p ? strlen(p) : 0) {}

	/**
	 * Create a slice object based on string
	 * @param str a constant string reference
	 */
	Slice(const string &str) : ptr_(str.data()), size_(str.size()) {}

	/**
	 * @public
	 * Get a pointer to an original character buffer
	 * @return a constant character buffer pointer
	 */
	const char *data() const { return ptr_; }

	/**
	 * @public
	 * Get saved size of an original character buffer
	 */
	size_t size() const { return size_; }

	/**
	 * @public
	 * Get an independent string based on an original character buffer with its size
	 * @return a string based on an original character buffer with its size
	 */
	string ToString() const { return string(ptr_, size_); }

protected:
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

}  // namespace reindexer
