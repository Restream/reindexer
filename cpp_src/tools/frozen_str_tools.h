#pragma once

#include <string_view>
#include "stringstools.h"
#include "vendor/frozen/bits/hash_string.h"
#include "vendor/frozen/string.h"

namespace frozen {

template <typename String>
constexpr std::size_t hash_ascii_string_nocase(const String& value, std::size_t seed) {
	std::size_t d = (0x811c9dc5 ^ seed) * static_cast<std::size_t>(0x01000193);
	for (const auto& c : value) {
		d = (d ^ (static_cast<std::size_t>(c) | 0x20)) * static_cast<std::size_t>(0x01000193);
	}
	return d >> 8;
}

struct [[nodiscard]] nocase_hash_str {
	constexpr std::size_t operator()(std::string_view hs, std::size_t seed) const noexcept { return hash_ascii_string_nocase(hs, seed); }
	constexpr std::size_t operator()(const frozen::string& hs, std::size_t seed) const noexcept {
		return hash_ascii_string_nocase(hs, seed);
	}
};

struct [[nodiscard]] nocase_equal_str {
	constexpr bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return reindexer::iequals(lhs, rhs); }
	constexpr bool operator()(const frozen::string& lhs, std::string_view rhs) const noexcept {
		return reindexer::iequals(std::string_view(lhs.data(), lhs.size()), rhs);
	}
	constexpr bool operator()(std::string_view lhs, const frozen::string& rhs) const noexcept {
		return reindexer::iequals(lhs, std::string_view(rhs.data(), rhs.size()));
	}
	constexpr bool operator()(const frozen::string& lhs, const frozen::string& rhs) const noexcept {
		return reindexer::iequals(std::string_view(lhs.data(), lhs.size()), std::string_view(rhs.data(), rhs.size()));
	}
};
}  // namespace frozen
