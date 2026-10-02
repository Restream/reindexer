#pragma once

#include <span>
#include <string_view>

namespace reindexer {

template <typename StrT, std::void_t<decltype(std::declval<StrT>().data())>* = nullptr>
inline std::span<char> giftStr(const StrT& s) noexcept {
	// Explicit const_cast for (s) to force COW-string copy
	return std::span<char>(const_cast<StrT&>(s).data(), s.size());
}
inline std::span<char> giftStr(std::string_view sv) noexcept { return std::span<char>(const_cast<char*>(sv.data()), sv.size()); }

}  // namespace reindexer
