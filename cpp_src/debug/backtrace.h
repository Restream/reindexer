#pragma once

#include <functional>
#include <string>
#include <string_view>

namespace reindexer {
namespace debug {

using backtrace_writer_t = std::function<void(std::string_view out)>;
using crash_query_reporter_t = std::function<void(std::ostream& sout)>;

void backtrace_init() noexcept;
void set_minidump_path(const std::string& p);
void backtrace_set_writer(backtrace_writer_t);
void backtrace_set_crash_query_reporter(crash_query_reporter_t);
backtrace_writer_t backtrace_get_writer();
crash_query_reporter_t backtrace_get_crash_query_reporter();
int backtrace_internal(void** addrlist, size_t size, void* ctx, std::string_view& method);
void backtrace_set_assertion_message(std::string&& msg) noexcept;
void print_backtrace(std::ostream& sout, void* ctx, int sig);
void print_crash_query(std::ostream& sout);
}  // namespace debug
}  // namespace reindexer
