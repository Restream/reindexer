#pragma once

#include <string>
#include <vector>

/**
 *  Converts number to text in Russian language.
 */
class NumToText {
public:
	static std::vector<std::string>& convert(const std::string& numStr, std::vector<std::string>& output);
};
