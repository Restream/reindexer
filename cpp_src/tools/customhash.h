#pragma once
#include <stdint.h>
#include <string>
#include "core/type_consts.h"

namespace reindexer {

uint32_t Hash(const std::wstring &s) noexcept;
uint32_t collateHash(std::string_view s, CollateMode collateMode) noexcept;
uint32_t HashTreGram(const wchar_t *ptr) noexcept;
uint32_t _Hash_bytes(const void *ptr, uint32_t len) noexcept;

}  // namespace reindexer
