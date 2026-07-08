#pragma once
#include <stdint.h>
#include <cstring>
#include <string>
#include "core/type_consts.h"
#include "estl/defines.h"

namespace reindexer {

RX_ALWAYS_INLINE uint32_t unaligned_load(const char* p) noexcept {
	uint32_t result;
	memcpy(&result, p, sizeof(result));
	return result;
}

inline uint32_t _Hash_bytes(const void* ptr, uint32_t len) noexcept {
	// Implementation of Murmur hash for 32-bit size_t.
	constexpr static uint32_t seed = 3339675911UL;
	constexpr static uint32_t m = 0x5bd1e995;
	uint32_t hash = seed ^ len;
	const char* buf = static_cast<const char*>(ptr);

	// Mix 4 bytes at a time into the hash.
	while (len >= 4) {
		uint32_t k = unaligned_load(buf);
		k *= m;
		k ^= k >> 24;
		k *= m;
		hash *= m;
		hash ^= k;
		buf += 4;
		len -= 4;
	}

	// Handle the last few bytes of the input array.
	if (len >= 3) {
		// TODO: Check with newer version. Clang-tidy v21 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		hash ^= static_cast<unsigned char>(buf[2]) << 16;
	}
	if (len >= 2) {
		// TODO: Check with newer version. Clang-tidy v21 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		hash ^= static_cast<unsigned char>(buf[1]) << 8;
	}
	if (len >= 1) {
		// TODO: Check with newer version. Clang-tidy v21 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		hash ^= static_cast<unsigned char>(buf[0]);
		hash *= m;
	}

	// Do a few final mixes of the hash.
	hash ^= hash >> 13;
	hash *= m;
	hash ^= hash >> 15;
	return hash;
}

uint32_t Hash(const std::wstring& s) noexcept;
template <CollateMode collateMode>
uint32_t collateHash(std::string_view s) noexcept;
template <>
uint32_t collateHash<CollateASCII>(std::string_view s) noexcept;
template <>
uint32_t collateHash<CollateUTF8>(std::string_view s) noexcept;
template <>
uint32_t collateHash<CollateCustom>(std::string_view s) noexcept;
template <>
inline uint32_t collateHash<CollateNone>(std::string_view s) noexcept {
	return _Hash_bytes(s.data(), s.length());
}
template <>
uint32_t collateHash<CollateNumeric>(std::string_view s) noexcept;
inline uint32_t collateHash(std::string_view s, CollateMode collateMode) noexcept {
	switch (collateMode) {
		case CollateASCII:
			return collateHash<CollateASCII>(s);
		case CollateUTF8:
			return collateHash<CollateUTF8>(s);
		case CollateCustom:
			return collateHash<CollateCustom>(s);
		case CollateNumeric:
			return collateHash<CollateNumeric>(s);
		case CollateNone:
		default:
			return collateHash<CollateNone>(s);
	}
}
uint32_t HashTreGram(const wchar_t* ptr) noexcept;

}  // namespace reindexer
