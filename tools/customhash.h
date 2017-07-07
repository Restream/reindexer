#pragma once
#include <stdint.h>
#include <string>

namespace search_engine {
using std::wstring;

uint32_t Hash(const wstring &s) noexcept;
uint32_t HashTreGram(const wchar_t *ptr) noexcept;
}  // namespace search_engine
