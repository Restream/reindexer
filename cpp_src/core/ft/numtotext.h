#pragma once

#include <string>
#include <string_view>
#include <vector>

/**
 *  Converts number to text in Russian language.
 */
namespace reindexer {
class [[nodiscard]] NumToText {
public:
	static std::vector<std::string_view>& convert(std::string_view numStr, std::vector<std::string_view>& output);
};

}  // namespace reindexer
