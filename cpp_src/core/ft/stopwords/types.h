#pragma once

#include <string>
#include "estl/fast_hash_set.h"
#include "tools/stringstools.h"

namespace reindexer {

struct [[nodiscard]] StopWord : std::string {
	enum class [[nodiscard]] Type { Stop, Morpheme };
	StopWord(std::string base, Type type = Type::Stop) noexcept : std::string(std::move(base)), type(type) {}
	Type type;
};

using word_hash = hash_str;
using word_equal = equal_str;
using word_less = less_str;
using StopWordsSetT = tsl::hopscotch_sc_set<StopWord, word_hash, word_equal, word_less>;

}  // namespace reindexer
