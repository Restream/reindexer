#pragma once

#include <spdlog/details/file_helper.h>
#include <spdlog/details/null_mutex.h>
#include <spdlog/details/synchronous_factory.h>
#include <spdlog/sinks/base_sink.h>
#include <shared_mutex>

namespace spdlog {
namespace sinks {

/*
 * File sink, that allows to manually reopen file
 * Thread safe
 */
class reopen_file_sink_mt final : public sink {
public:
	explicit reopen_file_sink_mt(const filename_t& filename, bool truncate = false) { file_helper_.open(filename, truncate); }

	reopen_file_sink_mt(const reopen_file_sink_mt&) = delete;
	reopen_file_sink_mt& operator=(const reopen_file_sink_mt&) = delete;

	void log(const details::log_msg& msg) final override {
		std::lock_guard lck(mtx_);
		if (reopen_) {
			file_helper_.reopen(false);
		}
		memory_buf_t formatted;
		formatter_->format(msg, formatted);
		file_helper_.write(formatted);
	}

	void flush() final override {
		std::lock_guard lck(mtx_);
		file_helper_.flush();
	}

	void reopen() noexcept {
		std::lock_guard lck(mtx_);
		reopen_ = true;
	}

	virtual void set_pattern(const std::string& pattern) final override {
		std::lock_guard lck(mtx_);
		formatter_ = details::make_unique<spdlog::pattern_formatter>(pattern);
	}
	virtual void set_formatter(std::unique_ptr<spdlog::formatter> sink_formatter) final override {
		std::lock_guard lck(mtx_);
		formatter_ = std::move(sink_formatter);
	}

private:
	std::unique_ptr<spdlog::formatter> formatter_ = details::make_unique<spdlog::pattern_formatter>();
	details::file_helper file_helper_;
	bool reopen_ = false;
	std::mutex mtx_;
};

class reopen_file_sink_st final : public sink {
public:
	explicit reopen_file_sink_st(const filename_t& filename, bool truncate = false) { file_helper_.open(filename, truncate); }

	reopen_file_sink_st(const reopen_file_sink_st&) = delete;
	reopen_file_sink_st& operator=(const reopen_file_sink_st&) = delete;

	void log(const details::log_msg& msg) final override {
		if (reopen_.load(std::memory_order_relaxed)) {
			reopen_.store(false, std::memory_order_relaxed);

			// Using mutex to avoid race on flush
			std::lock_guard lck(mtx_);
			file_helper_.reopen(false);
		}
		memory_buf_t formatted;
		formatter_->format(msg, formatted);
		file_helper_.write(formatted);
	}

	void flush() final override {
		std::lock_guard lck(mtx_);
		file_helper_.flush();
	}

	// This must be thread-safe
	void reopen() noexcept { reopen_.store(true, std::memory_order_relaxed); }

	virtual void set_pattern(const std::string& pattern) final override {
		formatter_ = details::make_unique<spdlog::pattern_formatter>(pattern);
	}
	virtual void set_formatter(std::unique_ptr<spdlog::formatter> sink_formatter) final override { formatter_ = std::move(sink_formatter); }

private:
	std::unique_ptr<spdlog::formatter> formatter_ = details::make_unique<spdlog::pattern_formatter>();
	details::file_helper file_helper_;
	std::atomic<bool> reopen_ = {false};
	std::mutex mtx_;
};

}  // namespace sinks
}  // namespace spdlog
