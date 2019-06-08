#pragma once

#include "estl/string_view.h"

using std::string;

namespace reindexer {

int64_t getTimeNow(string_view mode = "sec"_sv);

}  // namespace reindexer
