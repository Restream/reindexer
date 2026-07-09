#include "backtrace.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>
#include "estl/lock.h"
#include "estl/mutex.h"
#include "tools/fsops.h"

namespace reindexer {
namespace debug {

static recursive_mutex g_mutex;
static crash_query_reporter_t g_crash_query_reporter = [](std::ostream& sout) { sout << "<Empty crash query reporter>" << std::endl; };
static backtrace_writer_t g_writer = [](std::string_view sv) { std::cerr << sv; };
static std::string g_assertion_message("<empty>");

void backtrace_set_assertion_message(std::string&& msg) noexcept {
	lock_guard lck(g_mutex);
	g_assertion_message = std::move(msg);
}

static void print_assertion_message(std::ostream& sout) {
	std::string msg("<empty>");
	{
		lock_guard lck(g_mutex);
		std::swap(msg, g_assertion_message);
	}
	sout << "Assertion message: " << msg << std::endl;
}
}  // namespace debug
}  // namespace reindexer

#ifndef _WIN32
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <limits>
#include <span>
#include "resolver.h"

// There are 3 backtrace methods are available:
// 1. stangalone libunwind ( https://github.com/libunwind/libunwind )
// 2. libgcc's/llvm built in unwind
// 3. GNU's execinfo backtrace() call

#if REINDEX_WITH_LIBUNWIND
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#endif	// REINDEX_WITH_LIBUNWIND

#if REINDEX_WITH_UNWIND
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif	// _GNU_SOURCE
#include <setjmp.h>
#include <unwind.h>
#if defined(__linux__)
#include <sys/syscall.h>
#include <ucontext.h>
#endif	// defined(__linux__)
#endif	// REINDEX_WITH_UNWIND

#if REINDEX_WITH_EXECINFO
#include <execinfo.h>
#endif	// REINDEX_WITH_EXECINFO

#if REINDEX_OVERRIDE_ABORT
#include <syscall.h>
// Override abort/__assert_fail for musl build for correct backtrace
extern "C" void abort() {
	pid_t tid = syscall(SYS_gettid);
	syscall(SYS_tkill, tid, SIGABRT);
	for (;;) {
	}
}

extern "C" void __assert_fail(const char* expr, const char* file, int line, const char* func) {
	fprintf(stderr, "reindexer error: assertion failed: %s (%s: %s: %d)\n", expr, file, func, line);
	fflush(NULL);
	abort();
}
#endif

namespace reindexer {
namespace debug {

#if REINDEX_WITH_UNWIND
namespace {

#if defined(__linux__)

constexpr static size_t kMaxStackWindow = 8 * 1024 * 1024;

bool is_aligned_ptr(uintptr_t addr, size_t alignment) noexcept { return (addr % alignment) == 0; }

bool is_plausible_code_ptr(uintptr_t ip) noexcept { return ip >= 4096; }

thread_local sigjmp_buf g_frame_read_jmp;
thread_local volatile sig_atomic_t g_frame_read_active = 0;
static_assert(std::atomic<uintptr_t>::is_always_lock_free);
thread_local std::atomic<uintptr_t> g_frame_read_begin = 0;
thread_local std::atomic<uintptr_t> g_frame_read_end = 0;
// SIGSEGV/SIGBUS handlers are process-wide, so only one frame read may temporarily replace them.
// Do not block here: this path is reachable from a signal handler.
std::atomic_bool g_frame_read_sigaction_guard = {false};
std::atomic_bool g_frame_read_disabled = {false};
struct sigaction g_frame_read_segv_old{};
struct sigaction g_frame_read_bus_old{};

// Re-raise under the default disposition when we cannot safely continue or chain to the previous handler.
void redeliver_default_signal(int sig) noexcept {
	struct sigaction sa_default{};
	sa_default.sa_handler = SIG_DFL;
	sigemptyset(&sa_default.sa_mask);
	if (sigaction(sig, &sa_default, nullptr) != 0) {
		_exit(128 + sig);
	}

	sigset_t unblocked;
	sigemptyset(&unblocked);
	sigaddset(&unblocked, sig);
	if (sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) {
		_exit(128 + sig);
	}

	const pid_t pid = getpid();
	const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));
	if (syscall(SYS_tgkill, pid, tid, sig) != 0) {
		_exit(128 + sig);
	}
	for (;;) {
		pause();
	}
}

// A foreign signal means our temporary process-wide handlers are visible outside the protected read.
// Disable future use before forwarding so in-flight handlers cannot race with overwritten saved actions.
void forward_frame_read_fault(int sig, siginfo_t* info, void* ctx) noexcept {
	const int savedErrno = errno;
	g_frame_read_disabled.store(true, std::memory_order_release);
	const struct sigaction oldAction = sig == SIGBUS ? g_frame_read_bus_old : g_frame_read_segv_old;
	if (sigaction(sig, &oldAction, nullptr) != 0) {
		redeliver_default_signal(sig);
	}

	if (info && info->si_code > 0) {
		errno = savedErrno;
		return;
	}

	if (oldAction.sa_flags & SA_SIGINFO) {
		errno = savedErrno;
		oldAction.sa_sigaction(sig, info, ctx);
		errno = savedErrno;
		return;
	}
	if (oldAction.sa_handler == SIG_IGN) {
		errno = savedErrno;
		return;
	}
	if (oldAction.sa_handler == SIG_DFL) {
		redeliver_default_signal(sig);
	}
	errno = savedErrno;
	if (oldAction.sa_handler) {
		oldAction.sa_handler(sig);
		errno = savedErrno;
	}
}

// Only faults from the guarded two-word load are handled by siglongjmp; everything else is forwarded.
bool is_current_frame_read_fault(siginfo_t* info) noexcept {
	if (!g_frame_read_active || !info || info->si_code <= 0) {
		return false;
	}
	const uintptr_t faultAddr = reinterpret_cast<uintptr_t>(info->si_addr);
	const uintptr_t begin = g_frame_read_begin.load(std::memory_order_relaxed);
	const uintptr_t end = g_frame_read_end.load(std::memory_order_relaxed);
	return faultAddr >= begin && faultAddr < end;
}

void frame_read_fault_handler(int sig, siginfo_t* info, void* ctx) {
	if (is_current_frame_read_fault(info)) {
		siglongjmp(g_frame_read_jmp, 1);
	}
	forward_frame_read_fault(sig, info, ctx);
}

// Used before restoring handlers, so we do not overwrite a handler installed concurrently by another owner.
bool is_frame_read_handler(const struct sigaction& action) noexcept {
	return (action.sa_flags & SA_SIGINFO) != 0 && action.sa_sigaction == frame_read_fault_handler;
}

// Owns the temporary process-wide SIGSEGV/SIGBUS handlers around one frame read.
// It fails closed: restore errors or foreign signals disable this mechanism instead of risking handler corruption.
class [[nodiscard]] FrameReadHandlerGuard {
public:
	FrameReadHandlerGuard() noexcept {
		if (g_frame_read_disabled.load(std::memory_order_acquire) ||
			g_frame_read_sigaction_guard.exchange(true, std::memory_order_acquire)) {
			return;
		}
		locked_ = true;
		struct sigaction sa_new{};
		sa_new.sa_sigaction = frame_read_fault_handler;
		sigemptyset(&sa_new.sa_mask);
		sa_new.sa_flags = SA_SIGINFO | SA_ONSTACK;

		if (sigaction(SIGSEGV, &sa_new, &g_frame_read_segv_old) != 0) {
			return;
		}
		segv_installed_ = true;
		if (sigaction(SIGBUS, &sa_new, &g_frame_read_bus_old) != 0) {
			return;
		}
		bus_installed_ = true;
	}

	FrameReadHandlerGuard(const FrameReadHandlerGuard&) = delete;
	FrameReadHandlerGuard& operator=(const FrameReadHandlerGuard&) = delete;

	~FrameReadHandlerGuard() noexcept {
		if (locked_ && !restore()) {
			g_frame_read_disabled.store(true, std::memory_order_release);
		}
		if (locked_ && !g_frame_read_disabled.load(std::memory_order_acquire)) {
			g_frame_read_sigaction_guard.store(false, std::memory_order_release);
		}
	}

	bool restore() noexcept {
		if (restored_) {
			return true;
		}
		bool ok = true;
		if (bus_installed_) {
			ok = restore_if_unchanged(SIGBUS, g_frame_read_bus_old) && ok;
		}
		if (segv_installed_) {
			ok = restore_if_unchanged(SIGSEGV, g_frame_read_segv_old) && ok;
		}
		restored_ = true;
		return ok;
	}

	bool installed() const noexcept { return locked_ && segv_installed_ && bus_installed_; }

private:
	static bool restore_if_unchanged(int sig, const struct sigaction& oldAction) noexcept {
		struct sigaction current{};
		if (sigaction(sig, nullptr, &current) != 0) {
			return false;
		}
		if (!is_frame_read_handler(current)) {
			return false;
		}
		return sigaction(sig, &oldAction, nullptr) == 0;
	}

	bool locked_ = false;
	bool segv_installed_ = false;
	bool bus_installed_ = false;
	bool restored_ = false;
};

// Protects against SIGSEGV and SIGBUS in case of corrupted stack
bool try_read_frame_pair(uintptr_t frame_addr, uintptr_t& word0, uintptr_t& word1) noexcept {
	if (g_frame_read_active) {
		return false;
	}
	constexpr uintptr_t kFrameReadSize = 2 * sizeof(uintptr_t);
	if (frame_addr > std::numeric_limits<uintptr_t>::max() - kFrameReadSize) {
		return false;
	}
	FrameReadHandlerGuard handlerGuard;
	if (!handlerGuard.installed()) {
		return false;
	}

	g_frame_read_begin.store(frame_addr, std::memory_order_relaxed);
	g_frame_read_end.store(frame_addr + kFrameReadSize, std::memory_order_relaxed);
	g_frame_read_active = 1;
	volatile sig_atomic_t ok = 0;
	if (sigsetjmp(g_frame_read_jmp, 1) == 0) {
		const auto* frame = reinterpret_cast<const uintptr_t*>(frame_addr);
		word0 = frame[0];
		word1 = frame[1];
		ok = 1;
	}
	g_frame_read_active = 0;
	g_frame_read_begin.store(0, std::memory_order_relaxed);
	g_frame_read_end.store(0, std::memory_order_relaxed);
	return ok != 0;
}

#if defined(__x86_64__)
bool read_next_frame(uintptr_t bp, uintptr_t sp, uintptr_t& next_bp, uintptr_t& next_ip) noexcept {
	if (!is_aligned_ptr(bp, 8) || bp < sp || bp - sp > kMaxStackWindow) {
		return false;
	}
	uintptr_t frame_word0 = 0;
	uintptr_t frame_word1 = 0;
	if (!try_read_frame_pair(bp, frame_word0, frame_word1)) {
		return false;
	}
	next_bp = frame_word0;
	next_ip = frame_word1;
	if (!is_plausible_code_ptr(next_ip)) {
		return false;
	}
	if (next_bp != 0) {
		if (!is_aligned_ptr(next_bp, 8) || next_bp <= bp || next_bp < sp || next_bp - sp > kMaxStackWindow) {
			return false;
		}
	}
	return true;
}

size_t backtrace_from_ucontext(void* ctx, void** addrlist, size_t size) noexcept {
	auto* uc = static_cast<ucontext_t*>(ctx);
	uintptr_t ip = uc->uc_mcontext.gregs[REG_RIP];
	uintptr_t bp = uc->uc_mcontext.gregs[REG_RBP];
	const uintptr_t sp = uc->uc_mcontext.gregs[REG_RSP];
	size_t addrlen = 0;

	while (addrlen < size) {
		if (!is_plausible_code_ptr(ip)) {
			break;
		}
		addrlist[addrlen++] = reinterpret_cast<void*>(ip);

		uintptr_t next_bp = 0;
		uintptr_t next_ip = 0;
		if (!read_next_frame(bp, sp, next_bp, next_ip)) {
			break;
		}
		bp = next_bp;
		ip = next_ip;
		if (bp == 0) {
			break;
		}
	}
	return addrlen;
}

#elif defined(__aarch64__)

uintptr_t strip_return_address(uintptr_t ip) noexcept {
	return reinterpret_cast<uintptr_t>(__builtin_ptrauth_strip(reinterpret_cast<void*>(ip), 0));
}

bool read_next_frame(uintptr_t fp, uintptr_t sp, uintptr_t& next_fp, uintptr_t& next_ip) noexcept {
	if (!is_aligned_ptr(fp, 16) || fp < sp || fp - sp > kMaxStackWindow) {
		return false;
	}
	uintptr_t frame_word0 = 0;
	uintptr_t frame_word1 = 0;
	if (!try_read_frame_pair(fp, frame_word0, frame_word1)) {
		return false;
	}
	next_fp = frame_word0;
	next_ip = strip_return_address(frame_word1);
	if (!is_plausible_code_ptr(next_ip)) {
		return false;
	}
	if (next_fp != 0) {
		if (!is_aligned_ptr(next_fp, 16) || next_fp <= fp || next_fp < sp || next_fp - sp > kMaxStackWindow) {
			return false;
		}
	}
	return true;
}

size_t backtrace_from_ucontext(void* ctx, void** addrlist, size_t size) noexcept {
	auto* uc = static_cast<ucontext_t*>(ctx);
	uintptr_t ip = strip_return_address(uc->uc_mcontext.pc);
	uintptr_t fp = uc->uc_mcontext.regs[29];
	const uintptr_t sp = uc->uc_mcontext.sp;
	size_t addrlen = 0;

	while (addrlen < size) {
		if (!is_plausible_code_ptr(ip)) {
			break;
		}
		addrlist[addrlen++] = reinterpret_cast<void*>(ip);

		uintptr_t next_fp = 0;
		uintptr_t next_ip = 0;
		if (!read_next_frame(fp, sp, next_fp, next_ip)) {
			break;
		}
		fp = next_fp;
		ip = next_ip;
		if (fp == 0) {
			break;
		}
	}
	return addrlen;
}

#elif defined(__i386__)
bool read_next_frame(uintptr_t bp, uintptr_t sp, uintptr_t& next_bp, uintptr_t& next_ip) noexcept {
	if (!is_aligned_ptr(bp, 4) || bp < sp || bp - sp > kMaxStackWindow) {
		return false;
	}
	uintptr_t frame_word0 = 0;
	uintptr_t frame_word1 = 0;
	if (!try_read_frame_pair(bp, frame_word0, frame_word1)) {
		return false;
	}
	next_bp = frame_word0;
	next_ip = frame_word1;
	if (!is_plausible_code_ptr(next_ip)) {
		return false;
	}
	if (next_bp != 0) {
		if (!is_aligned_ptr(next_bp, 4) || next_bp <= bp || next_bp < sp || next_bp - sp > kMaxStackWindow) {
			return false;
		}
	}
	return true;
}

size_t backtrace_from_ucontext(void* ctx, void** addrlist, size_t size) noexcept {
	auto* uc = static_cast<ucontext_t*>(ctx);
	uintptr_t ip = uc->uc_mcontext.gregs[REG_EIP];
	uintptr_t bp = uc->uc_mcontext.gregs[REG_EBP];
	const uintptr_t sp = uc->uc_mcontext.gregs[REG_ESP];
	size_t addrlen = 0;

	while (addrlen < size) {
		if (!is_plausible_code_ptr(ip)) {
			break;
		}
		addrlist[addrlen++] = reinterpret_cast<void*>(ip);

		uintptr_t next_bp = 0;
		uintptr_t next_ip = 0;
		if (!read_next_frame(bp, sp, next_bp, next_ip)) {
			break;
		}
		bp = next_bp;
		ip = next_ip;
		if (bp == 0) {
			break;
		}
	}
	return addrlen;
}

#else	// !defined(__i386__)
size_t backtrace_from_ucontext(void* /*ctx*/, void** /*addrlist*/, size_t /*size*/) noexcept { return 0; }
#endif	// !defined(__i386__)

#else	// !defined(__linux__)
size_t backtrace_from_ucontext(void* /*ctx*/, void** /*addrlist*/, size_t /*size*/) noexcept { return 0; }
#endif	// !defined(__linux__)

class [[nodiscard]] Unwinder {
public:
	size_t operator()(std::span<void*> trace) {
		trace_ = trace;
		index_ = 0;
		skip_innermost_ = true;
		_Unwind_Backtrace(&this->backtrace_trampoline, this);
		return static_cast<size_t>(index_);
	}

private:
	static _Unwind_Reason_Code backtrace_trampoline(_Unwind_Context* ctx, void* self) {
		return (static_cast<Unwinder*>(self))->backtrace(ctx);
	}

	_Unwind_Reason_Code backtrace(_Unwind_Context* ctx) {
		if (skip_innermost_) {
			skip_innermost_ = false;
			return _URC_NO_REASON;
		}
		if (size_t(index_) >= trace_.size()) {
			return _URC_END_OF_STACK;
		}

		int ip_before_instruction = 0;
		uintptr_t ip = _Unwind_GetIPInfo(ctx, &ip_before_instruction);

		if (!ip_before_instruction) {
			if (ip == 0) {
				ip = std::numeric_limits<uintptr_t>::max();
			} else {
				ip -= 1;
			}
		}

		trace_[index_++] = reinterpret_cast<void*>(ip);
		return _URC_NO_REASON;
	}
	ssize_t index_;
	std::span<void*> trace_;
	bool skip_innermost_ = false;
};

}  // namespace
#endif

int backtrace_internal(void** addrlist, size_t size, void* ctx, std::string_view& method) {
	using namespace std::string_view_literals;
	size_t addrlen = 0;
	(void)size;
	(void)method;
	(void)addrlist;

#if REINDEX_WITH_LIBUNWIND
	method = "libunwind"sv;
	unw_cursor_t cursor;
	unw_context_t uc;

	if (!ctx) {
		unw_getcontext(&uc);
		ctx = &uc;
	}

	unw_init_local(&cursor, reinterpret_cast<unw_context_t*>(ctx));

	addrlen = 0;
	do {
		unw_word_t ip;
		unw_get_reg(&cursor, UNW_REG_IP, &ip);
		addrlist[addrlen++] = reinterpret_cast<void*>(ip);
	} while (unw_step(&cursor) && addrlen < size);
#endif	// REINDEX_WITH_LIBUNWIND
#if REINDEX_WITH_UNWIND
	if (addrlen < 2) {
		method = "unwind"sv;
		if (ctx) {
			addrlen = backtrace_from_ucontext(ctx, addrlist, size);
		}
		if (addrlen < 2) {
			Unwinder unw;
			addrlen = unw(std::span<void*>(addrlist, size));
		}
	}
#endif	// REINDEX_WITH_UNWIND
#if REINDEX_WITH_EXECINFO
	if (addrlen < 2) {
		method = "execinfo"sv;
		addrlen = ::backtrace(addrlist, size);
	}
#endif	// REINDEX_WITH_EXECINFO
	return addrlen;
}

void print_backtrace(std::ostream& sout, void* ctx, int sig) {
#if !REINDEX_WITH_EXECINFO && !REINDEX_WITH_UNWIND && !REINDEX_WITH_LIBUNWIND
	sout << "Sorry, reindexer has been compiled without any backtrace methods." << std::endl;
#else	// REINDEX_WITH_EXECINFO || REINDEX_WITH_UNWIND || REINDEX_WITH_LIBUNWIND
	void* addrlist[64] = {};
	auto resolver = TraceResolver::New();
	std::string_view method;
	int addrlen = backtrace_internal(addrlist, sizeof(addrlist) / sizeof(addrlist[0]), ctx, method);

	if (sig >= 0) {
		sout << "Signal " << sig << " ";
	}
	sout << "backtrace (" << method << "):" << std::endl;
	for (int i = 0; i < addrlen; i++) {
		auto te = TraceEntry(uintptr_t(addrlist[i]));
		std::ignore = resolver->Resolve(te);
		sout << " #" << (i + 1) << " " << te << std::endl;
	}
#endif	// REINDEX_WITH_EXECINFO || REINDEX_WITH_UNWIND || REINDEX_WITH_LIBUNWIND
}

void print_crash_query(std::ostream& sout) {
	auto crash_query_reporter = backtrace_get_crash_query_reporter();
	if (crash_query_reporter) {
		crash_query_reporter(sout);
	} else {
		sout << "<No crash query reporter set>" << std::endl;
	}
}

static void sighandler(int sig, siginfo_t*, void* ctx) {
	const auto writer = backtrace_get_writer();
	std::ostringstream sout;
	sout << "*** Backtrace on signal: " << sig << " ***" << std::endl;
	writer(sout.str());
	sout.str(std::string());
	sout.clear();
	print_crash_query(sout);
	writer(sout.str());
	sout.str(std::string());
	sout.clear();
	print_assertion_message(sout);
	writer(sout.str());
	sout.str(std::string());
	sout.clear();
	print_backtrace(sout, ctx, sig);
	writer(sout.str());

	exit(-1);
}

void backtrace_init() noexcept {
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_sigaction = sighandler;
	sa.sa_flags = SA_SIGINFO | SA_NODEFER | SA_RESETHAND;
	sigaction(SIGSEGV, &sa, nullptr);
	sigaction(SIGABRT, &sa, nullptr);
	sigaction(SIGBUS, &sa, nullptr);
}

void set_minidump_path(const std::string&) { assert(false); }

void backtrace_set_writer(backtrace_writer_t writer) {
	lock_guard lck(g_mutex);
	g_writer = std::move(writer);
}
void backtrace_set_crash_query_reporter(crash_query_reporter_t reporter) {
	lock_guard lck(g_mutex);
	g_crash_query_reporter = std::move(reporter);
}
backtrace_writer_t backtrace_get_writer() {
	lock_guard lck(g_mutex);
	return g_writer;
}
crash_query_reporter_t backtrace_get_crash_query_reporter() {
	lock_guard lck(g_mutex);
	return g_crash_query_reporter;
}

}  // namespace debug
}  // namespace reindexer

#elif defined(_WIN32) && defined(REINDEX_WITH_CPPTRACE)

#include <windows.h>
// Headers order matters: windows.h must be included before dbghelp.h
#include <dbghelp.h>
#undef min
#undef max
#include <csignal>
#include <memory>
#include "cpptrace/cpptrace.hpp"
#include "tools/clock.h"

namespace reindexer {
namespace debug {
static std::string g_pathMiniDump;

void outputDebugInfo(const backtrace_writer_t& writer, EXCEPTION_POINTERS* ExceptionInfo) {
	std::ostringstream sout;
	print_crash_query(sout);
	writer(sout.str());
	sout.str(std::string());
	sout.clear();
	print_assertion_message(sout);
	writer(sout.str());
	sout.str(std::string());
	sout.clear();
	print_backtrace(sout, nullptr, 0);
	writer(sout.str());

	const auto tm = system_clock_w::now();
	const auto duration = tm.time_since_epoch();
	int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

	std::string fileName = fs::JoinPath(g_pathMiniDump, std::string("crash") + std::to_string(now) + ".dmp");
	HANDLE hFile = CreateFile(fileName.c_str(), GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
	std::shared_ptr<void> fileClose{hFile, CloseHandle};
	if (ExceptionInfo) {
		MINIDUMP_EXCEPTION_INFORMATION mei;
		mei.ThreadId = GetCurrentThreadId();
		mei.ClientPointers = TRUE;
		mei.ExceptionPointers = ExceptionInfo;
		MiniDumpWriteDump(GetCurrentProcess(), GetCurrentProcessId(), hFile, MiniDumpNormal, &mei, NULL, NULL);
	} else {
		MiniDumpWriteDump(GetCurrentProcess(), GetCurrentProcessId(), hFile, MiniDumpNormal, NULL, NULL, NULL);
	}

	sout.clear();
	sout << "MiniDump file " << fileName.c_str() << std::endl;
	writer(sout.str());
}

LONG WINAPI exceptionHandler(PEXCEPTION_POINTERS pExceptionInfo) {
	const auto writer = backtrace_get_writer();
	std::ostringstream sout;
	sout << "*** Backtrace on: " << std::hex << pExceptionInfo->ExceptionRecord->ExceptionCode << std::dec << " ***" << std::endl;
	writer(sout.str());
	outputDebugInfo(writer, pExceptionInfo);
	return EXCEPTION_CONTINUE_SEARCH;
}

void signalAbortHandler(int /*signal*/) {
	const auto writer = backtrace_get_writer();
	std::ostringstream sout;
	sout << "*** Backtrace on: abort SIGNAL ***" << std::endl;
	writer(sout.str());
	outputDebugInfo(writer, nullptr);
}

void backtrace_init() noexcept {
	std::signal(SIGABRT, signalAbortHandler);
	SetUnhandledExceptionFilter(exceptionHandler);
}

void set_minidump_path(const std::string& p) { g_pathMiniDump = p; }

void backtrace_set_writer(backtrace_writer_t writer) {
	lock_guard lck(g_mutex);
	g_writer = std::move(writer);
}
int backtrace_internal(void**, size_t, void*, std::string_view&) { return 0; }
void backtrace_set_crash_query_reporter(crash_query_reporter_t reporter) {
	lock_guard lck(g_mutex);
	g_crash_query_reporter = std::move(reporter);
}
backtrace_writer_t backtrace_get_writer() {
	lock_guard lck(g_mutex);
	return g_writer;
}
crash_query_reporter_t backtrace_get_crash_query_reporter() {
	lock_guard lck(g_mutex);
	return g_crash_query_reporter;
}
void print_backtrace(std::ostream& sout, void*, int) { cpptrace::generate_trace().print(sout, false); }
void print_crash_query(std::ostream& sout) {
	auto reporter = backtrace_get_crash_query_reporter();
	if (reporter) {
		reporter(sout);
	}
}

}  // namespace debug
}  // namespace reindexer
#else
namespace reindexer {
namespace debug {
static recursive_mutex g_mutex;
static crash_query_reporter_t g_crash_query_reporter = [](std::ostream&) {};
static backtrace_writer_t g_writer = [](std::string_view sv) { std::cerr << sv; };

void backtrace_init() noexcept {}
void set_minidump_path(const std::string&) { assert(false); }
void backtrace_set_writer(backtrace_writer_t) {}
int backtrace_internal(void**, size_t, void*, std::string_view&) { return 0; }
void backtrace_set_crash_query_reporter(crash_query_reporter_t reporter) {
	lock_guard lck(g_mutex);
	g_crash_query_reporter = std::move(reporter);
}
backtrace_writer_t backtrace_get_writer() {
	lock_guard lck(g_mutex);
	return g_writer;
}
crash_query_reporter_t backtrace_get_crash_query_reporter() {
	lock_guard lck(g_mutex);
	return g_crash_query_reporter;
}
void print_backtrace(std::ostream&, void*, int) {}
void print_crash_query(std::ostream& sout) {
	auto reporter = backtrace_get_crash_query_reporter();
	if (reporter) {
		reporter(sout);
	}
}

}  // namespace debug
}  // namespace reindexer
#endif	// defined(_WIN32) && defined(REINDEX_WITH_CPPTRACE)
