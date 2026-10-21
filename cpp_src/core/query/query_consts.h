#pragma once

#include <limits>

namespace reindexer {

static constexpr unsigned kQueryMaxLimit = std::numeric_limits<unsigned>::max();
static constexpr unsigned kQueryMinOffset = 0;

}  // namespace reindexer
