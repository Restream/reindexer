#pragma once

#include <memory>
#include <string_view>
#include <vector>

namespace reindexer {
namespace json_string {

// JSON-string may have 2 formats:
// 1. In-place. In case, when string is shorter than 8 MB, it may be stored inside initial JSON-document. Raw pointer ('p') points to the
// beggining of the string header.
// Bytes:               |p-length...p-1|p...p+2|
// ...<JSON document>...| string data  | length|...<JSON document>
// 2. Allocated. If string is larger than 8 MB, header contains pointer to actual string.
// Bytes:               |p-9...     p-2|p-1...p+2|
// ...<JSON document>...|pointer to str| length  |...<JSON document>
// Type of the string is set by byte at p+2:
// 0xxxxxxx - In-place string;
// 1xxxxxxx - Allocated string.
constexpr static unsigned kLargeJSONStrFlag = 0x80;

template <bool isLargeString>
inline size_t length(const uint8_t* p) noexcept {
	if constexpr (isLargeString) {
		return p[-1] | (unsigned(p[0]) << 8) | (unsigned(p[1]) << 16) | ((unsigned(p[2]) & ~kLargeJSONStrFlag) << 24);
	} else {
		return p[0] | (unsigned(p[1]) << 8) | (unsigned(p[2]) << 16);
	}
}

inline size_t length(const uint8_t* p) noexcept {
	// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound) Uncheked access
	if (p[2] & kLargeJSONStrFlag) {
		return length<true>(p);
	}
	return length<false>(p);
}

inline std::string_view to_string_view(const uint8_t* p) noexcept {
	// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound) Uncheked access
	if (p[2] & kLargeJSONStrFlag) {
		const auto len = length<true>(p);
		uintptr_t uptr;
		static_assert(sizeof(uintptr_t) == 8 || sizeof(uintptr_t) == 4, "Expecting sizeof uintptr to be equal 4 or 8 bytes");
#if UINTPTR_MAX == 0xFFFFFFFF
		uptr = uintptr_t(p[-2]) | (uintptr_t(p[-3]) << 8) | (uintptr_t(p[-4]) << 16) | (uintptr_t(p[-5]) << 24);
#elif UINTPTR_MAX == 0xFFFFFFFFFFFFFFFF
		uptr = uintptr_t(p[-2]) | (uintptr_t(p[-3]) << 8) | (uintptr_t(p[-4]) << 16) | (uintptr_t(p[-5]) << 24) | (uintptr_t(p[-6]) << 32) |
			   (uintptr_t(p[-7]) << 40) | (uintptr_t(p[-8]) << 48) | (uintptr_t(p[-9]) << 56);
#else
		static_assert(false, "Unexpected uintptr_t size");
#endif
		return std::string_view(reinterpret_cast<const char*>(uptr), len);
	}
	const auto len = length<false>(p);
	return std::string_view(reinterpret_cast<const char*>(p - len), len);
}

inline void encode(uint8_t* p, uint64_t l, std::vector<std::unique_ptr<char[]>>& storage) {
	if (l >= (uint64_t(1) << 23)) {
		storage.emplace_back(new char[l]);
		std::copy(p - l, p, storage.back().get());
		const auto uptr = uintptr_t(storage.back().get());
		// Put length
		p[-1] = l & 0xFF;
		p[0] = (l >> 8) & 0xFF;
		p[1] = (l >> 16) & 0xFF;
		p[2] = ((l >> 24) & 0xFF) | kLargeJSONStrFlag;
		// Put pointer
		p[-2] = uptr & 0xFF;
		p[-3] = (uptr >> 8) & 0xFF;
		p[-4] = (uptr >> 16) & 0xFF;
		p[-5] = (uptr >> 24) & 0xFF;
#if UINTPTR_MAX == 0xFFFFFFFF
#elif UINTPTR_MAX == 0xFFFFFFFFFFFFFFFF
		p[-6] = (uptr >> 32) & 0xFF;
		p[-7] = (uptr >> 40) & 0xFF;
		p[-8] = (uptr >> 48) & 0xFF;
		p[-9] = (uptr >> 56) & 0xFF;
#else
		static_assert(false, "Unexpected uintptr_t size");
#endif
	} else {
		// Put length
		p[0] = l & 0xFF;
		p[1] = (l >> 8) & 0xFF;
		p[2] = (l >> 16) & 0xFF;
	}
}

}  // namespace json_string
}  // namespace reindexer
