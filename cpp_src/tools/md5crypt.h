#pragma once

#include "estl/string_view.h"
#include "tools/errors.h"

namespace reindexer {

constexpr auto kMD5CryptMagic = "1"_sv;

std::string MD5crypt(const std::string &passwd, const std::string &salt) noexcept;
Error ParseMd5CryptString(const std::string &input, std::string &outHash, std::string &outSalt);

}  // namespace reindexer
