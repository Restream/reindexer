#pragma once

#include "tools/errors.h"
#include "vendor/expected/expected.h"

namespace reindexer {

template <typename T>
using Expected = nonstd::expected<T, Error>;

using Unexpected = nonstd::unexpected_type<Error>;

}  // namespace reindexer
