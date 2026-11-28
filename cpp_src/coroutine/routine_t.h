#pragma once

#include <cstdint>

namespace reindexer::coroutine {

/// Coroutine identifier. 0 is reserved (main routine / "empty" value)
using routine_t = uint32_t;

}  // namespace reindexer::coroutine
