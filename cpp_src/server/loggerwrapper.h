#pragma once

#include <string_view>
#include "spdlog/spdlog.h"

namespace reindexer_server {

using std::shared_ptr;

class LoggerWrapper {
public:
	LoggerWrapper() = default;
	LoggerWrapper(const char *name) : logger_(spdlog::get(name)) {}

	operator bool() const noexcept { return logger_ != nullptr; }

	template <typename... Args>
	void error(Args &&...args) const {
		if (logger_) {
			logger_->error(std::forward<Args>(args)...);
		}
	}

	template <typename... Args>
	void warn(Args &&...args) const {
		if (logger_) {
			logger_->warn(std::forward<Args>(args)...);
		}
	}

	template <typename... Args>
	void info(Args &&...args) const {
		if (logger_) {
			logger_->info(std::forward<Args>(args)...);
		}
	}

	template <typename... Args>
	void trace(Args &&...args) const {
		if (logger_) {
			logger_->trace(std::forward<Args>(args)...);
		}
	}

	template <typename... Args>
	void critical(Args &&...args) const {
		if (logger_) {
			logger_->critical(std::forward<Args>(args)...);
		}
	}

	template <typename... Args>
	void debug(Args &&...args) const {
		if (logger_) {
			logger_->debug(std::forward<Args>(args)...);
		}
	}

private:
	std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace reindexer_server

namespace fmt {
static inline void format_arg(fmt::BasicFormatter<char> &f, const char *&, std::string_view s) {
	f.writer() << fmt::BasicStringRef<char>(s.data(), s.length());
}
}  // namespace fmt
