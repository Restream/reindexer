#pragma once

#ifndef _MSC_VER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#else
#pragma warning(push)
#pragma warning(disable : 4267 4146)
#endif

#include <cstdint>
#include <cstring>

#include "estl/defines.h"

/**
 * Return the ZigZag-encoded 32-bit unsigned integer form of a 32-bit signed
 * integer.
 *
 * \param v
 *      Value to encode.
 * \return
 *      ZigZag encoded integer.
 */
RX_ALWAYS_INLINE uint32_t zigzag32(int32_t v) noexcept { return (v < 0) ? (((-(uint32_t)v) << 1) - 1) : ((uint32_t)(v) << 1); }

/**
 * Return the ZigZag-encoded 64-bit unsigned integer form of a 64-bit signed
 * integer.
 *
 * \param v
 *      Value to encode.
 * \return
 *      ZigZag encoded integer.
 */
RX_ALWAYS_INLINE uint64_t zigzag64(int64_t v) noexcept { return (v < 0) ? (((-(uint64_t)v) << 1) - 1) : ((uint64_t)(v) << 1); }

/**
 * Pack an unsigned 32-bit integer in base-128 varint encoding and return the
 * number of bytes written, which must be 5 or less.
 *
 * \param value
 *      Value to encode.
 * \param[out] out
 *      Packed value.
 * \return
 *      Number of bytes written to `out`.
 */
inline size_t uint32_pack(uint32_t value, uint8_t *out) noexcept {
	unsigned rv = 0;

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
	/* assert: value<128 */
	out[rv++] = value;
	return rv;
}

/**
 * Pack a signed 32-bit integer and return the number of bytes written.
 * Negative numbers are encoded as two's complement 64-bit integers.
 *
 * \param value
 *      Value to encode.
 * \param[out] out
 *      Packed value.
 * \return
 *      Number of bytes written to `out`.
 */
inline size_t int32_pack(int32_t value, uint8_t *out) noexcept {
	if (value < 0) {
		out[0] = value | 0x80;
		out[1] = (value >> 7) | 0x80;
		out[2] = (value >> 14) | 0x80;
		out[3] = (value >> 21) | 0x80;
		out[4] = (value >> 28) | 0x80;
		out[5] = out[6] = out[7] = out[8] = 0xff;
		out[9] = 0x01;
		return 10;
	} else {
		return uint32_pack(value, out);
	}
}

/**
 * Pack a signed 32-bit integer using ZigZag encoding and return the number of
 * bytes written.
 *
 * \param value
 *      Value to encode.
 * \param[out] out
 *      Packed value.
 * \return
 *      Number of bytes written to `out`.
 */
inline size_t sint32_pack(int32_t value, uint8_t *out) noexcept { return uint32_pack(zigzag32(value), out); }

/**
 * Pack a 64-bit unsigned integer using base-128 varint encoding and return the
 * number of bytes written.
 *
 * \param value
 *      Value to encode.
 * \param[out] out
 *      Packed value.
 * \return
 *      Number of bytes written to `out`.
 */
inline size_t uint64_pack(uint64_t value, uint8_t *out) noexcept {
	uint32_t hi = (uint32_t)(value >> 32);
	uint32_t lo = (uint32_t)value;
	unsigned rv;

	if (hi == 0) return uint32_pack((uint32_t)lo, out);
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
	rv = 5;
	while (hi >= 128) {
		out[rv++] = hi | 0x80;
		hi >>= 7;
	}
	out[rv++] = hi;
	return rv;
}

/**
 * Pack a 64-bit signed integer in ZigZag encoding and return the number of
 * bytes written.
 *
 * \param value
 *      Value to encode.
 * \param[out] out
 *      Packed value.
 * \return
 *      Number of bytes written to `out`.
 */
inline size_t sint64_pack(int64_t value, uint8_t *out) noexcept { return uint64_pack(zigzag64(value), out); }

inline size_t boolean_pack(bool value, uint8_t *out) noexcept {
	*out = value ? 1 : 0;
	return 1;
}

inline size_t string_pack(const char *str, uint8_t *out) noexcept {
	if (str == nullptr) {
		out[0] = 0;
		return 1;
	} else {
		size_t len = strlen(str);
		size_t rv = uint32_pack(len, out);
		memcpy(out + rv, str, len);
		return rv + len;
	}
}

inline size_t string_pack(const char *str, const size_t len, uint8_t *out) noexcept {
	if (str == nullptr) {
		out[0] = 0;
		return 1;
	} else {
		size_t rv = uint32_pack(len, out);
		memcpy(out + rv, str, len);
		return rv + len;
	}
}

inline uint32_t parse_uint32(unsigned len, const uint8_t *data) noexcept {
	uint32_t rv = data[0] & 0x7f;
	if (len > 1) {
		rv |= ((uint32_t)(data[1] & 0x7f) << 7);
		if (len > 2) {
			rv |= ((uint32_t)(data[2] & 0x7f) << 14);
			if (len > 3) {
				rv |= ((uint32_t)(data[3] & 0x7f) << 21);
				if (len > 4) rv |= ((uint32_t)(data[4]) << 28);
			}
		}
	}
	return rv;
}

inline uint32_t parse_int32(unsigned len, const uint8_t *data) noexcept { return parse_uint32(len, data); }

RX_ALWAYS_INLINE int32_t unzigzag32(uint32_t v) noexcept { return (v & 1) ? (-(v >> 1) - 1) : (v >> 1); }

inline uint32_t parse_fixed_uint32(const uint8_t *data) noexcept {
	uint32_t t;
	memcpy(&t, data, 4);
	return t;
}

inline uint64_t parse_uint64(unsigned len, const uint8_t *data) noexcept {
	unsigned shift, i;
	uint64_t rv;

	if (len < 5) return parse_uint32(len, data);
	rv = ((uint64_t)(data[0] & 0x7f)) | ((uint64_t)(data[1] & 0x7f) << 7) | ((uint64_t)(data[2] & 0x7f) << 14) |
		 ((uint64_t)(data[3] & 0x7f) << 21);
	shift = 28;
	for (i = 4; i < len; i++) {
		rv |= (((uint64_t)(data[i] & 0x7f)) << shift);
		shift += 7;
	}
	return rv;
}

RX_ALWAYS_INLINE int64_t unzigzag64(uint64_t v) noexcept { return (v & 1) ? (-(v >> 1) - 1) : (v >> 1); }

inline uint64_t parse_fixed_uint64(const uint8_t *data) noexcept {
	uint64_t t;
	memcpy(&t, data, 8);
	return t;
}

inline unsigned scan_varint(unsigned len, const uint8_t *data) noexcept {
	unsigned i;
	if (len > 10) len = 10;
	for (i = 0; i < len; i++)
		if ((data[i] & 0x80) == 0) break;
	if (i == len) return 0;
	return i + 1;
}

#ifndef _MSC_VER
#pragma GCC diagnostic pop
#else
#pragma warning(push)
#endif
