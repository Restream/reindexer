#pragma once

#include <functional>
#include <string_view>

namespace reindexer {
namespace debug {
void backtrace_init();
void backtrace_set_writer(std::function<void(std::string_view out)>);
void backtrace_set_crash_query_reporter(std::function<void(std::ostream &sout)>);
int backtrace_internal(void **addrlist, size_t size, void *ctx, std::string_view &method);
void print_backtrace(std::ostream &sout, void *ctx, int sig);
void print_crash_query(std::ostream &sout);
}  // namespace debug
}  // namespace reindexer
