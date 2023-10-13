#pragma once
#include <stdint.h>
#include <string>
#include "core/type_consts.h"

namespace reindexer {

uint32_t Hash(const std::wstring &s) noexcept;
template <CollateMode collateMode>
uint32_t collateHash(std::string_view s) noexcept;
template <>
uint32_t collateHash<CollateASCII>(std::string_view s) noexcept;
template <>
uint32_t collateHash<CollateUTF8>(std::string_view s) noexcept;
template <>
uint32_t collateHash<CollateCustom>(std::string_view s) noexcept;
template <>
uint32_t collateHash<CollateNone>(std::string_view s) noexcept;
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
		case CollateNone:
			return collateHash<CollateNone>(s);
		case CollateNumeric:
			return collateHash<CollateNumeric>(s);
	}
	return collateHash<CollateNone>(s);
}
uint32_t HashTreGram(const wchar_t *ptr) noexcept;
uint32_t _Hash_bytes(const void *ptr, uint32_t len) noexcept;

}  // namespace reindexer
