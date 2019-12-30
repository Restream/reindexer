#pragma once

#include "estl/string_view.h"

namespace reindexer {

struct TerminalSize {
	int width = 0;
	int height = 0;
};
TerminalSize getTerminalSize();
int getStringTerminalWidth(string_view str);
bool isStdoutRedirected();

}  // namespace reindexer
