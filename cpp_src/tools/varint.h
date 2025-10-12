#pragma once

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4267 4146)
#endif

#include <cstdint>
#include <cstring>

#include "estl/defines.h"

// ZigZag Transform: Encodes signed integers so that they can be effectively used with varint encoding.
//
// varint operates on unsigned integers, encoding smaller numbers into fewer bytes.
// If you try to use it on a signed integer, it will treat this number as a very large unsigned integer,
// which means that even small signed numbers like -1 will take the maximum number of bytes (10) to encode.
// zigzag_encode() maps signed integers to unsigned in such a way that those with a small absolute value
// will have smaller encoded values, making them appropriate for encoding using varint.
//
//     int32_t ->     uint32_t
// -------------------------
//           0 ->          0
//          -1 ->          1
//           1 ->          2
//          -2 ->          3
//         ... ->        ...
//  2147483647 -> 4294967294
// -2147483648 -> 4294967295
//
//        >> encode >>
//        << decode <<
RX_ALWAYS_INLINE uint32_t zigzag_encode32(int32_t n) noexcept {
	// NOTE: the right-shift must be arithmetic
	// NOTE: left shift must be unsigned because of overflow
	return (static_cast<uint32_t>(n) << 1) ^ static_cast<uint32_t>(n >> 31);
}
RX_ALWAYS_INLINE uint64_t zigzag_encode64(int64_t n) noexcept {
	// NOTE: the right-shift must be arithmetic
	// NOTE: left shift must be unsigned because of overflow
	return (static_cast<uint64_t>(n) << 1) ^ static_cast<uint64_t>(n >> 63);
}
RX_ALWAYS_INLINE int64_t zigzag_decode64(uint64_t n) noexcept {
	// NOTE: using unsigned types prevent undefined behavior
	return static_cast<int64_t>((n >> 1) ^ (~(n & 1) + 1));
}

// Pack an unsigned 32-bit integer in base-128 varint encoding and return the number of bytes written, which must be 5 or less
inline size_t uint32_pack(uint32_t value, uint8_t* out) noexcept {
	size_t rv = 0;
	if (value >= 0x80) {
		out[rv++] = value | 0x80;
		value >>= 7;
		if (value >= 0x80) {
			out[rv++] = value | 0x80;
			value >>= 7;
			if (value >= 0x80) {
				out[rv++] = value | 0x80;
				value >>= 7;
				if (value >= 0x80) {
					out[rv++] = value | 0x80;
					value >>= 7;
				}
			}
		}
	}
	// assert: value<128
	out[rv++] = value;
	return rv;
}

// Pack a signed 32-bit integer using ZigZag encoding and return the number of bytes written
inline size_t sint32_pack(int32_t value, uint8_t* out) noexcept { return uint32_pack(zigzag_encode32(value), out); }

// Pack a 64-bit unsigned integer using base-128 varint encoding and return the number of bytes written
inline size_t uint64_pack(uint64_t value, uint8_t* out) noexcept {
	auto hi = static_cast<uint32_t>(value >> 32);
	const auto lo = static_cast<uint32_t>(value);
	if (hi == 0) {
		return uint32_pack(lo, out);
	}
	out[0] = (lo) | 0x80;
	out[1] = (lo >> 7) | 0x80;
	out[2] = (lo >> 14) | 0x80;
	out[3] = (lo >> 21) | 0x80;
	if (hi < 8) {
		out[4] = (hi << 4) | (lo >> 28);
		return 5;
	} else {
		out[4] = ((hi & 7) << 4) | (lo >> 28) | 0x80;
		hi >>= 3;
	}
	size_t rv = 5;
	while (hi >= 128) {
		out[rv++] = hi | 0x80;
		hi >>= 7;
	}
	out[rv++] = hi;
	return rv;
}

// Pack a 64-bit signed integer in ZigZag encoding and return the number of bytes written
inline size_t sint64_pack(int64_t value, uint8_t* out) noexcept { return uint64_pack(zigzag_encode64(value), out); }

inline size_t boolean_pack(bool value, uint8_t* out) noexcept {
	*out = value ? 1 : 0;
	return 1;
}

inline size_t string_pack(const char* str, const size_t len, uint8_t* out) noexcept {
	if (str == nullptr) {
		out[0] = 0;
		return 1;
	}
	size_t rv = uint32_pack(len, out);
	memcpy(out + rv, str, len);
	return rv + len;
}

inline uint32_t parse_uint32(unsigned len, const uint8_t* data) noexcept {
	uint32_t rv = data[0] & 0x7f;
	if (len > 1) {
		// TODO: Check with newer version. Clang-tidy v21 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		rv |= (static_cast<uint32_t>(data[1] & 0x7f) << 7);
		if (len > 2) {
			// TODO: Check with newer version. Clang-tidy v21 is unable to validate len correctly
			// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
			rv |= (static_cast<uint32_t>(data[2] & 0x7f) << 14);
			if (len > 3) {
				// TODO: Check with newer version. Clang-tidy v21 is unable to validate len correctly
				// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
				rv |= (static_cast<uint32_t>(data[3] & 0x7f) << 21);
				if (len > 4) {
					// TODO: Check with newer version. Clang-tidy v21 is unable to validate len correctly
					// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
					rv |= (static_cast<uint32_t>(data[4]) << 28);
				}
			}
		}
	}
	return rv;
}

inline uint64_t parse_uint64(unsigned len, const uint8_t* data) noexcept {
	if (len < 5) {
		return parse_uint32(len, data);
	}
	uint64_t rv = (static_cast<uint64_t>(data[0] & 0x7f)) | (static_cast<uint64_t>(data[1] & 0x7f) << 7) |
				  (static_cast<uint64_t>(data[2] & 0x7f) << 14) | (static_cast<uint64_t>(data[3] & 0x7f) << 21);
	size_t shift = 28;
	for (size_t i = 4; i < len; ++i) {
		rv |= ((static_cast<uint64_t>(data[i] & 0x7f)) << shift);
		shift += 7;
	}
	return rv;
}

inline int64_t parse_int64(unsigned len, const uint8_t* data) noexcept { return zigzag_decode64(parse_uint64(len, data)); }

inline unsigned scan_varint(unsigned len, const uint8_t* data) noexcept {
	if (len > 10) {
		len = 10;
	}
	unsigned i = 0;
	for (; i < len; ++i) {
		// TODO: Check with newer version. Clang-tidy v21 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		if ((data[i] & 0x80) == 0) {
			break;
		}
	}
	return (i == len) ? 0 : i + 1;
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif
