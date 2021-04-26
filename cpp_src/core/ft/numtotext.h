#pragma once

#include <string>
#include <string_view>
#include <vector>

/**
 *  Converts number to text in Russian language.
 */
namespace reindexer {
class NumToText {
public:
	static std::vector<std::string>& convert(std::string_view numStr, std::vector<std::string>& output);
};

}  // namespace reindexer
