#include "progress.h"

#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <unistd.h>
#endif

#include "tools/terminalutils.h"

namespace reindexer_tool {

constexpr std::string_view kAnsiControlSequenceIntroducer = "\033[";
constexpr std::string_view kAnsiClearLine = "\033[2K";
constexpr char kAnsiCursorUp = 'A';
constexpr char kAnsiCursorRight = 'C';

static std::string progressBar(size_t current, size_t total, size_t width) {
	std::string res;
	res.reserve(width + 2);
	res.push_back('[');
	const size_t filled = total == 0 ? width : std::min(width, static_cast<size_t>((static_cast<long double>(current) * width) / total));
	res.append(filled, '#');
	res.append(width - filled, '-');
	res.push_back(']');
	return res;
}

static double percent(size_t current, size_t total) noexcept { return total == 0 ? 100.0 : 100.0 * double(current) / double(total); }

static std::string percentString(double percent) {
	std::ostringstream out;
	out << std::fixed << std::setprecision(1) << std::setw(5) << percent << '%';
	return out.str();
}

static std::string makeProgressLine(std::string_view title, size_t current, size_t total) {
	constexpr size_t kBarWidth = 20;
	current = std::min(current, total);

	std::string line(title);
	line += ": ";
	line += progressBar(current, total, kBarWidth);
	line += ' ';
	line += percentString(percent(current, total));
	line += " (";
	line += std::to_string(current);
	line += '/';
	line += std::to_string(total);
	line += ')';
	return line;
}

static std::vector<std::string> makeProgressLines(std::string_view title, std::span<const ProgressInfo> progresses) {
	constexpr size_t kMaxActiveProcesses = 3;

	size_t processed = 0;
	size_t total = 0;
	std::vector<std::string> lines;
	lines.reserve(progresses.size() + 1);
	size_t activeCount = 0;
	for (const auto& info : progresses) {
		processed += std::min(info.processed, info.total);
		total += info.total;
		if (info.activeWorkers == 0 || info.total == 0 || activeCount >= kMaxActiveProcesses) {
			continue;
		}
		lines.emplace_back(makeProgressLine("  " + info.name, info.processed, info.total));
		++activeCount;
	}
	if (total == 0) {
		return {};
	}
	lines.insert(lines.begin(), makeProgressLine(title, processed, total));
	return lines;
}

static std::optional<size_t> terminalWidthFromEnv() {
	if (const char* columns = std::getenv("COLUMNS")) {
		char* end = nullptr;
		const long value = std::strtol(columns, &end, 10);
		if (end != columns && value > 0) {
			return static_cast<size_t>(value);
		}
	}
	return std::nullopt;
}

static size_t terminalWidth() {
#ifdef _WIN32
	CONSOLE_SCREEN_BUFFER_INFO info;
	if (GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &info)) {
		return std::max<size_t>(1, info.srWindow.Right - info.srWindow.Left + 1);
	}
#else
	winsize ws;
	if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0) {
		return ws.ws_col;
	}
#endif
	return terminalWidthFromEnv().value_or(80);
}

static std::string fitLine(std::string_view line, size_t maxWidth) {
	if (line.size() <= maxWidth) {
		return std::string(line);
	}
	if (maxWidth <= 3) {
		return std::string(line.substr(0, maxWidth));
	}
	std::string res(line.substr(0, maxWidth - 3));
	res += "...";
	return res;
}

ConsoleProgress::ConsoleProgress() noexcept
	: lastPrint_(std::chrono::steady_clock::now() - kUpdateInterval),
	  interactive_(!reindexer::isStdoutRedirected() && reindexer::isStdoutAnsiSupported()) {}

void ConsoleProgress::Print(std::string_view title, size_t current, size_t total) {
	print(std::vector{makeProgressLine(title, current, total)});
}

void ConsoleProgress::Print(std::string_view title, std::span<const ProgressInfo> progressEntities) {
	print(makeProgressLines(title, progressEntities));
}

void ConsoleProgress::print(std::vector<std::string>&& lines) {
	if (!interactive_) {
		return;
	}
	if (lines.empty()) {
		return;
	}
	const auto now = std::chrono::steady_clock::now();
	if (now - lastPrint_ < kUpdateInterval) {
		return;
	}
	lastPrint_ = now;

	const size_t maxLineWidth = std::max<size_t>(1, terminalWidth() - 1);
	for (auto& line : lines) {
		line = fitLine(line, maxLineWidth);
	}

	/**
	 * In short, the redraw algorithm is:
	 * 1. Return to the beginning of the old progress block.
	 * 2. Clear and print the new lines.
	 * 3. If the old block was longer than the new one, clear the remaining old lines.
	 * 4. Return the cursor to the end of the last new line.
	 */
	const size_t oldLineCount = lastLineLens_.size();

	// Move the cursor to the beginning of the previously printed progress block.
	std::cout << '\r';
	if (oldLineCount > 1) {
		std::cout << kAnsiControlSequenceIntroducer << (oldLineCount - 1) << kAnsiCursorUp;
	}

	// Rewrite actual progress lines from the top of the old block.
	for (size_t i = 0; i < lines.size(); ++i) {
		std::cout << kAnsiClearLine << '\r' << lines[i];
		if (i + 1 < lines.size()) {
			std::cout << '\n';
		}
	}

	if (oldLineCount > lines.size()) {
		const size_t extraLines = oldLineCount - lines.size();
		// Clear trailing lines left from the previous, longer progress block.
		for (size_t i = 0; i < extraLines; ++i) {
			std::cout << '\n' << kAnsiClearLine << '\r';
		}
		// Return the cursor to the end of the last currently visible progress line.
		std::cout << kAnsiControlSequenceIntroducer << extraLines << kAnsiCursorUp;
		if (!lines.back().empty()) {
			std::cout << kAnsiControlSequenceIntroducer << lines.back().size() << kAnsiCursorRight;
		}
	}

	std::cout << std::flush;
	lastLineLens_.resize(lines.size());
	for (size_t i = 0; i < lines.size(); ++i) {
		lastLineLens_[i] = lines[i].size();
	}
}

void ConsoleProgress::Done(std::string_view message) {
	if (!interactive_) {
		std::cout << message << std::endl;
		return;
	}
	// To ensure that the message will be printed we set the last print time to the past
	lastPrint_ -= kUpdateInterval;
	print(std::vector{std::string{message}});
	std::cout << std::endl;
}

}  // namespace reindexer_tool
