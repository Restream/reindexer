#ifndef JSTEEMANN_ATOI_H
#define JSTEEMANN_ATOI_H 1

#pragma once

#include <limits>
#include <type_traits>

// some macros to help with branch prediction
#if defined(__GNUC__) || defined(__GNUG__)
#define ATOI_LIKELY(v) __builtin_expect(!!(v), 1)
#define ATOI_UNLIKELY(v) __builtin_expect(!!(v), 0)
#else
#define ATOI_LIKELY(v) v
#define ATOI_UNLIKELY(v) v
#endif

namespace jsteemann {

// low-level worker function to convert the string value between p
// (inclusive) and e (exclusive) into a negative number value of type T,
// without validation of the input string - use this only for trusted input!
//
// the input string will always be interpreted as a base-10 number.
// expects the input string to contain only the digits '0' to '9'.
// there is no validation of the input string, and overflow or underflow
// of the result value will not be detected.
// this function will not modify errno.
template <typename T>
inline T atoi_negative_unchecked(char const* p, char const* e) noexcept {
	T result = 0;
	while (p != e) {
		result = (result << 1) + (result << 3) - (*(p++) - '0');
	}
	return result;
}

// low-level worker function to convert the string value between p
// (inclusive) and e (exclusive) into a positive number value of type T,
// without validation of the input string - use this only for trusted input!
//
// the input string will always be interpreted as a base-10 number.
// expects the input string to contain only the digits '0' to '9'.
// there is no validation of the input string, and overflow or underflow
// of the result value will not be detected.
// this function will not modify errno.
template <typename T>
inline T atoi_positive_unchecked(char const* p, char const* e) noexcept {
	T result = 0;
	while (p != e) {
		result = (result << 1) + (result << 3) + *(p++) - '0';
	}

	return result;
}

// function to convert the string value between p
// (inclusive) and e (exclusive) into a number value of type T, without
// validation of the input string - use this only for trusted input!
//
// the input string will always be interpreted as a base-10 number.
// expects the input string to contain only the digits '0' to '9'. an
// optional '+' or '-' sign is allowed too.
// there is no validation of the input string, and overflow or underflow
// of the result value will not be detected.
// this function will not modify errno.
template <typename T>
inline T atoi_unchecked(char const* p, char const* e) noexcept {
	if (ATOI_UNLIKELY(p == e)) {
		return T();
	}

	if (*p == '-') {
		if (!std::is_signed<T>::value) {
			return T();
		}
		return atoi_negative_unchecked<T>(++p, e);
	}
	if (ATOI_UNLIKELY(*p == '+')) {
		++p;
	}

	return atoi_positive_unchecked<T>(p, e);
}

// low-level worker function to convert the string value between p
// (inclusive) and e (exclusive) into a negative number value of type T
//
// the input string will always be interpreted as a base-10 number.
// expects the input string to contain only the digits '0' to '9'.
// if any other character is found, the output parameter "valid" will
// be set to false. if the parsed value is less than what type T can
// store without truncation, "valid" will also be set to false.
// this function will not modify errno.
template <typename T>
inline T atoi_negative(char const* p, char const* e, bool& valid) noexcept {
	if (ATOI_UNLIKELY(p == e)) {
		valid = false;
		return T();
	}

	constexpr T cutoff = (std::numeric_limits<T>::min)() / 10;
	constexpr char cutlim = -((std::numeric_limits<T>::min)() % 10);
	T result = 0;

	do {
		char c = *p;
		// we expect only '0' to '9'. everything else is unexpected
		if (ATOI_UNLIKELY(c < '0' || c > '9')) {
			valid = false;
			return result;
		}

		c -= '0';
		// we expect the bulk of values to not hit the bounds restrictions
		if (ATOI_UNLIKELY(result < cutoff || (result == cutoff && c > cutlim))) {
			valid = false;
			return result;
		}
		result *= 10;
		result -= c;
	} while (++p < e);

	valid = true;
	return result;
}

// low-level worker function to convert the string value between p
// (inclusive) and e (exclusive) into a positive number value of type T
//
// the input string will always be interpreted as a base-10 number.
// expects the input string to contain only the digits '0' to '9'.
// if any other character is found, the output parameter "valid" will
// be set to false. if the parsed value is greater than what type T can
// store without truncation, "valid" will also be set to false.
// this function will not modify errno.
template <typename T>
inline T atoi_positive(char const* p, char const* e, bool& valid) noexcept {
	if (ATOI_UNLIKELY(p == e)) {
		valid = false;
		return T();
	}

	constexpr T cutoff = (std::numeric_limits<T>::max)() / 10;
	constexpr char cutlim = (std::numeric_limits<T>::max)() % 10;
	T result = 0;

	do {
		char c = *p;

		// we expect only '0' to '9'. everything else is unexpected
		if (ATOI_UNLIKELY(c < '0' || c > '9')) {
			valid = false;
			return result;
		}

		c -= '0';
		// we expect the bulk of values to not hit the bounds restrictions
		if (ATOI_UNLIKELY(result > cutoff || (result == cutoff && c > cutlim))) {
			valid = false;
			return result;
		}
		result *= 10;
		result += c;
	} while (++p < e);

	valid = true;
	return result;
}

// function to convert the string value between p
// (inclusive) and e (exclusive) into a number value of type T
//
// the input string will always be interpreted as a base-10 number.
// expects the input string to contain only the digits '0' to '9'. an
// optional '+' or '-' sign is allowed too.
// if any other character is found, the output parameter "valid" will
// be set to false. if the parsed value is less or greater than what
// type T can store without truncation, "valid" will also be set to
// false.
// this function will not modify errno.
template <typename T>
inline typename std::enable_if<std::is_signed<T>::value, T>::type atoi(char const* p, char const* e, bool& valid) noexcept {
	if (ATOI_UNLIKELY(p == e)) {
		valid = false;
		return T();
	}

	if (*p == '-') {
		return atoi_negative<T>(++p, e, valid);
	}
	if (ATOI_UNLIKELY(*p == '+')) {
		++p;
	}

	return atoi_positive<T>(p, e, valid);
}

template <typename T>
inline typename std::enable_if<std::is_unsigned<T>::value, T>::type atoi(char const* p, char const* e, bool& valid) noexcept {
	if (ATOI_UNLIKELY(p == e)) {
		valid = false;
		return T();
	}

	if (*p == '-') {
		valid = false;
		return T();
	}
	if (ATOI_UNLIKELY(*p == '+')) {
		++p;
	}

	return atoi_positive<T>(p, e, valid);
}

}  // namespace jsteemann

#endif