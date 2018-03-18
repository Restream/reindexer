#pragma once

#include "spdlog/spdlog.h"

namespace reindexer_server {

using std::shared_ptr;

class LoggerWrapper {
public:
	LoggerWrapper() {}
	LoggerWrapper(const char *name) : logger_(spdlog::get(name)) {}

	operator bool() { return logger_ != nullptr; }

	template <typename... Args>
	void error(const Args &... args) {
		if (logger_) logger_->error(args...);
	}

	template <typename... Args>
	void warn(const Args &... args) {
		if (logger_) logger_->warn(args...);
	}

	template <typename... Args>
	void info(const Args &... args) {
		if (logger_) logger_->info(args...);
	}

	template <typename... Args>
	void trace(const Args &... args) {
		if (logger_) logger_->trace(args...);
	}

	template <typename... Args>
	void critical(const Args &... args) {
		if (logger_) logger_->critical(args...);
	}

	template <typename... Args>
	void debug(const Args &... args) {
		if (logger_) logger_->debug(args...);
	}

private:
	shared_ptr<spdlog::logger> logger_;
};

}  // namespace reindexer_server
