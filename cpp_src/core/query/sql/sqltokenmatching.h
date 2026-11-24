#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include "sqltokentype.h"
#include "tools/stringstools.h"

namespace reindexer {

using SqlTokenVariants = std::unordered_set<std::string, nocase_hash_str, nocase_equal_str>;
using SqlTokenMatchings = std::unordered_map<SqlTokenType, SqlTokenVariants>;

const SqlTokenMatchings& sqlTokenMatchings();
void getMatchingSqlTokens(SqlTokenType tokenType, const std::string& token, std::unordered_set<std::string>& variants);

}  // namespace reindexer
