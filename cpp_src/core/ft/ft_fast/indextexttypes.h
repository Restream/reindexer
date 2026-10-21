#pragma once

#include <cstdint>
#include <limits>
#include "estl/fast_hash_map.h"

namespace reindexer {

using WordIdType = uint32_t;

using FoundWordsProcsType = fast_hash_map<WordIdType, float>;
using FoundWordsType = fast_hash_map<WordIdType, size_t>;

enum class [[nodiscard]] FtUseExternStatuses : bool { Yes, No };

}  // namespace reindexer
