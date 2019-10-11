#pragma once

#include <functional>
#include "estl/string_view.h"

namespace reindexer {
namespace debug {
void backtrace_init();
void backtrace_set_writer(std::function<void(string_view out)>);
int backtrace_internal(void **addrlist, size_t size, void *ctx, string_view &method);
}  // namespace debug
}  // namespace reindexer
