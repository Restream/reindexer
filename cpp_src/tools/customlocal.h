#pragma once
#include <string>

namespace reindexer {

void ToLower(std::wstring& data) noexcept;
wchar_t ToLower(wchar_t ch) noexcept;

bool IsAlpha(wchar_t ch) noexcept;
inline bool IsDigit(wchar_t ch) noexcept { return ch >= '0' && ch <= '9'; }

}  // namespace reindexer
