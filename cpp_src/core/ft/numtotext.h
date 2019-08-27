#pragma once

#include <string>
#include <vector>
#include "estl/string_view.h"

/**
 *  Converts number to text in Russian language.
 */
namespace reindexer {
class NumToText {
public:
	static std::vector<std::string>& convert(string_view numStr, std::vector<std::string>& output);
};

}  // namespace reindexer