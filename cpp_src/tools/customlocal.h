#pragma once
#include <string>

namespace reindexer {

using std::wstring;

void ToLower(wstring& data) noexcept;
wchar_t ToLower(wchar_t ch) noexcept;

bool IsAlpha(wchar_t ch) noexcept;
bool IsDigit(wchar_t ch) noexcept;
}  // namespace reindexer
