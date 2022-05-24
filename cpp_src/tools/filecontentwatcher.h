#pragma once

#include <chrono>
#include <mutex>
#include "fsops.h"

namespace reindexer {

class FileContetWatcher {
public:
	FileContetWatcher(std::string filename, std::function<Error(const std::string&)> loadFromYaml,
					  std::function<Error(const std::string&)> loadFromFile) noexcept
		: filename_(std::move(filename)), loadFromYaml_(std::move(loadFromYaml)), loadFromFile_(std::move(loadFromFile)) {
		assertrx(loadFromYaml_);
		assertrx(loadFromYaml_);
	}
	void SetDirectory(const std::string& directory) noexcept {
		assertrx(!hasFilepath_.load(std::memory_order_acquire));
		filepath_ = fs::JoinPath(directory, filename_);
		auto stat = fs::StatTime(filepath_);
		lastReplConfMTime_.store(stat.mtime, std::memory_order_relaxed);
		hasFilepath_.store(true, std::memory_order_release);
	}

	Error Enable() noexcept {
		if (!hasFilepath_.load(std::memory_order_acquire)) {
			return Error(errLogic, "Filepath for FileMTimeChecker is not set");
		}
		isEnabled_.store(true, std::memory_order_release);
		return errOK;
	}

	void Check() {
		std::string yamlReplConf;
		const bool wasModified = readIfFileWasModified(yamlReplConf);
		if (wasModified) {
			hadErrorOnLastTry_ = !(loadFromYaml_(yamlReplConf).ok());
		} else if (hadErrorOnLastTry_) {
			// Retry to read error config once
			// This logic adds delay between write and read, which allows writer to finish all his writes
			hadErrorOnLastTry_ = false;
			loadFromFile_(filename_);
		}
	}

	template <typename PredicatT>
	Error RewriteFile(std::string content, PredicatT hasSameContent) {
		if (!isEnabled_.load(std::memory_order_acquire) || !hasFilepath_.load(std::memory_order_acquire)) {
			return errOK;
		}

		std::string tmpPath = filepath_ + ".tmp";
		std::string curContent;
		auto res = fs::ReadFile(filepath_, curContent);
		if (res < 0) {
			return errOK;
		}
		if (hasSameContent(curContent)) {
			return errOK;
		}
		std::lock_guard<std::mutex> lck(mtx_);
		res = fs::WriteFile(tmpPath, content);
		if (res < 0 || static_cast<size_t>(res) != content.size()) {
			return Error(errParams, "Unable to write tmp file [%s]. Reason: %s", tmpPath, strerror(errno));
		}
		res = fs::Rename(tmpPath, filepath_);
		if (res < 0) {
			return Error(errParams, "Unable to rename tmp file from [%s] to [%s]. Reason: %s", tmpPath, filepath_, strerror(errno));
		}
		expectedContent_ = std::move(content);
		return errOK;
	}

	std::string_view Filename() const noexcept { return filename_; }

private:
	bool readIfFileWasModified(std::string& content) {
		if (!isEnabled_.load(std::memory_order_acquire) || !hasFilepath_.load(std::memory_order_acquire)) {
			return false;
		}
		auto stat = fs::StatTime(filepath_);
		if (stat.mtime > 0) {
			auto lastReplConfMTime = lastReplConfMTime_.load(std::memory_order_acquire);
			if (lastReplConfMTime != stat.mtime) {
				if (lastReplConfMTime_.compare_exchange_strong(lastReplConfMTime, stat.mtime, std::memory_order_acq_rel)) {
					auto res = fs::ReadFile(filepath_, content);
					if (res < 0) {
						content.clear();
					}
					std::lock_guard<std::mutex> lck(mtx_);
					if (content != expectedContent_) {
						return true;
					}
				}
			}
		}
		return false;
	}

	const std::string filename_;
	std::string filepath_;
	std::string expectedContent_;
	std::atomic<bool> hasFilepath_{false};
	std::atomic<bool> isEnabled_{false};
	std::atomic<int64_t> lastReplConfMTime_{-1};
	std::mutex mtx_;
	std::function<Error(const std::string&)> loadFromYaml_;
	std::function<Error(const std::string&)> loadFromFile_;
	bool hadErrorOnLastTry_ = false;
};

}  // namespace reindexer
