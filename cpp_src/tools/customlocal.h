#pragma once
#include <string>

namespace reindexer {

using std::wstring;

void ToLower(wstring& data);
wchar_t ToLower(wchar_t ch);

bool IsAlpha(wchar_t ch);
bool IsDigit(wchar_t ch);
}  // namespace reindexer
