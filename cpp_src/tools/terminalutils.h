#pragma once

#include <string_view>

namespace reindexer {

struct [[nodiscard]] TerminalSize {
	int width = 0;
	int height = 0;
};
TerminalSize getTerminalSize();
int getStringTerminalWidth(std::string_view str);
bool isStdoutRedirected();
bool isStdinRedirected();
}  // namespace reindexer
