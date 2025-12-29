#pragma once

#include <atomic>
#include <string>
#include "core/type_consts.h"
#include "tools/assertrx.h"

namespace reindexer {
namespace cluster {

#if defined(rtfmt) || defined(rtstr) || defined(logTrace) || defined(logInfo) || defined(logWarn) || defined(logError)
static_assert(false, "Macros conflict");
#endif

#define rtfmt(f, ...) return fmt::format("[cluster:{}] " f, logModuleName(), __VA_ARGS__)
#define rtstr(f) return fmt::format("[cluster:{}] " f, logModuleName())
#define logTraceSimple(str) log_.Trace([] { rtstr(str); }, __FILE__, __LINE__, __FUNCTION__)
#define logInfoSimple(str) log_.Info([] { rtstr(str); }, __FILE__, __LINE__, __FUNCTION__)
#define logWarnSimple(str) log_.Warn([] { rtstr(str); }, __FILE__, __LINE__, __FUNCTION__)
#define logErrorSimple(str) log_.Error([] { rtfmt(str); }, __FILE__, __LINE__, __FUNCTION__)
#define logTrace(f, ...) log_.Trace([&] { rtfmt(f, __VA_ARGS__); }, __FILE__, __LINE__, __FUNCTION__)
#define logInfo(f, ...) log_.Info([&] { rtfmt(f, __VA_ARGS__); }, __FILE__, __LINE__, __FUNCTION__)
#define logWarn(f, ...) log_.Warn([&] { rtfmt(f, __VA_ARGS__); }, __FILE__, __LINE__, __FUNCTION__)
#define logError(f, ...) log_.Error([&] { rtfmt(f, __VA_ARGS__); }, __FILE__, __LINE__, __FUNCTION__)

class [[nodiscard]] Logger {
public:
	Logger(LogLevel minOutputLogLevel = LogInfo) noexcept : minOutputLogLevel_(minOutputLogLevel) {}

	void SetLevel(LogLevel l) noexcept { level_.store(l, std::memory_order_relaxed); }
	LogLevel GetLevel() const noexcept { return level_.load(std::memory_order_relaxed); }

	template <typename F>
	void Error(F&& f, const char* file, int line, const char* func) const noexcept {
		try {
			Log(LogError, std::forward<F>(f));
		} catch (std::exception& e) {
			fprintf(stderr, "reindexer error: Unexpected exception during logging: '%s'\nLocation:'%s:%s:%d'\n", e.what(),
					file ? file : "null", func ? func : "null", line);
			assertrx_dbg(false);
		}
	}
	template <typename F>
	void Warn(F&& f, const char* file, int line, const char* func) const noexcept {
		try {
			Log(LogWarning, std::forward<F>(f));
		} catch (std::exception& e) {
			fprintf(stderr, "reindexer error: Unexpected exception during logging: '%s'\nLocation:'%s:%s:%d'\n", e.what(),
					file ? file : "null", func ? func : "null", line);
			assertrx_dbg(false);
		}
	}
	template <typename F>
	void Info(F&& f, const char* file, int line, const char* func) const noexcept {
		try {
			Log(LogInfo, std::forward<F>(f));
		} catch (std::exception& e) {
			fprintf(stderr, "reindexer error: Unexpected exception during logging: '%s'\nLocation:'%s:%s:%d'\n", e.what(),
					file ? file : "null", func ? func : "null", line);
			assertrx_dbg(false);
		}
	}
	template <typename F>
	void Trace(F&& f, const char* file, int line, const char* func) const noexcept {
		try {
			Log(LogTrace, std::forward<F>(f));
		} catch (std::exception& e) {
			fprintf(stderr, "reindexer error: Unexpected exception during logging: '%s'\nLocation:'%s:%s:%d'\n", e.what(),
					file ? file : "null", func ? func : "null", line);
			assertrx_dbg(false);
		}
	}
	template <typename F>
	void Log(LogLevel l, F&& f) const {
		if (l <= GetLevel()) {
			std::string str = f();
			if (!str.empty()) {
				const auto outLevel = minOutputLogLevel_ < l ? minOutputLogLevel_ : l;
				print(outLevel, str);
			}
		}
	}

private:
	void print(LogLevel l, std::string& str) const;

	std::atomic<LogLevel> level_;
	const LogLevel minOutputLogLevel_;
};

}  // namespace cluster
}  // namespace reindexer
