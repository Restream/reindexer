#pragma once
#include <string>

namespace reindexer {

using MaskingFunc = std::string (*)(std::string_view);

std::string maskLogin(std::string_view);
std::string maskPassword(std::string_view);

}  // namespace reindexer