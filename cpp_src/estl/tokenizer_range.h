#pragma once

#include <cstddef>

namespace reindexer {

struct [[nodiscard]] TokenizerRange {
	size_t lineStart{0};
	size_t columnStart{0};
	size_t lineEnd{0};
	size_t columnEnd{0};
};

}  // namespace reindexer
