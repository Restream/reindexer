#pragma once

#include <optional>
#include <string>
#include <vector>
#include "estl/tokenizer_range.h"

namespace reindexer {

struct [[nodiscard]] SQLSuggestions {
	std::vector<std::string> suggestions;
	std::optional<TokenizerRange> errorRange;
	std::string errorMessage;
};

}  // namespace reindexer
