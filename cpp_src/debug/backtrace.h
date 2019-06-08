#pragma once

#include <functional>
#include "estl/string_view.h"

namespace reindexer {
namespace debug {
void backtrace_init();
void backtrace_set_writer(std::function<void(string_view out)>);
}  // namespace debug
}  // namespace reindexer
