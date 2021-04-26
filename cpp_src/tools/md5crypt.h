#pragma once

#include <string_view>
#include "tools/errors.h"

namespace reindexer {

constexpr std::string_view kMD5CryptMagic{"1"};

std::string MD5crypt(const std::string &passwd, const std::string &salt) noexcept;
Error ParseMd5CryptString(const std::string &input, std::string &outHash, std::string &outSalt);

}  // namespace reindexer
