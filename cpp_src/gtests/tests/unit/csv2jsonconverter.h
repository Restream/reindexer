#pragma once

#include <string_view>
#include <vector>

namespace reindexer_tests {

std::vector<std::string> parseCSVRow(std::string_view row);
std::string csv2json(std::string_view row, const std::vector<std::string>& schema);

}  // namespace reindexer_tests
