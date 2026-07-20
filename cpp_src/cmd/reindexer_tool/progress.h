#pragma once

#include <chrono>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace reindexer_tool {

struct [[nodiscard]] ProgressInfo {
	std::string name;
	size_t processed = 0;
	size_t total = 0;
	size_t activeWorkers = 0;
};

class [[nodiscard]] ConsoleProgress {
public:
	ConsoleProgress() noexcept;

	void Print(std::string_view title, size_t current, size_t total);
	void Print(std::string_view title, std::span<const ProgressInfo> progressEntities);
	void Done(std::string_view message);

private:
	static constexpr std::chrono::milliseconds kUpdateInterval{200};

	void print(std::vector<std::string>&& lines);

	std::vector<size_t> lastLineLens_;
	std::chrono::steady_clock::time_point lastPrint_;
	bool interactive_;
};

}  // namespace reindexer_tool
