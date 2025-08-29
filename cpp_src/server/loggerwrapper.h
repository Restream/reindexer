#pragma once

#include <string_view>
#include "spdlog/spdlog.h"

namespace reindexer_server {

class [[nodiscard]] LoggerWrapper {
public:
	LoggerWrapper() = default;
	LoggerWrapper(const char* name) : logger_(spdlog::get(name)) {}

	operator bool() const noexcept { return logger_ != nullptr; }

	template <typename... Args>
	void error(spdlog::format_string_t<Args...> fmt, Args&&... args) const {
		if (logger_) {
			logger_->error(fmt, std::forward<Args>(args)...);
		}
	}
	template <typename T>
	void error(const T& msg) const {
		if (logger_) {
			logger_->error(msg);
		}
	}

	template <typename... Args>
	void warn(spdlog::format_string_t<Args...> fmt, Args&&... args) const {
		if (logger_) {
			logger_->warn(fmt, std::forward<Args>(args)...);
		}
	}
	template <typename T>
	void warn(const T& msg) const {
		if (logger_) {
			logger_->warn(msg);
		}
	}

	template <typename... Args>
	void info(spdlog::format_string_t<Args...> fmt, Args&&... args) const {
		if (logger_) {
			logger_->info(fmt, std::forward<Args>(args)...);
		}
	}
	template <typename T>
	void info(const T& msg) const {
		if (logger_) {
			logger_->info(msg);
		}
	}

	template <typename... Args>
	void trace(spdlog::format_string_t<Args...> fmt, Args&&... args) const {
		if (logger_) {
			logger_->trace(fmt, std::forward<Args>(args)...);
		}
	}
	template <typename T>
	void trace(const T& msg) const {
		if (logger_) {
			logger_->trace(msg);
		}
	}

	template <typename... Args>
	void critical(spdlog::format_string_t<Args...> fmt, Args&&... args) const {
		if (logger_) {
			logger_->critical(fmt, std::forward<Args>(args)...);
		}
	}
	template <typename T>
	void critical(const T& msg) const {
		if (logger_) {
			logger_->critical(msg);
		}
	}

	template <typename... Args>
	void debug(spdlog::format_string_t<Args...> fmt, Args&&... args) const {
		if (logger_) {
			logger_->debug(fmt, std::forward<Args>(args)...);
		}
	}
	template <typename T>
	void debug(const T& msg) const {
		if (logger_) {
			logger_->debug(msg);
		}
	}

private:
	std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace reindexer_server
