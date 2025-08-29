#pragma once

#include <span>
#include "tools/errors.h"

namespace reindexer {
class WrSerializer;
}

namespace reindexer_tool {

struct [[nodiscard]] DumpOptions {
	enum class Mode { FullNode, ShardedOnly, LocalOnly };

	Mode mode;

	static Mode ModeFromStr(std::string_view mode);
	static std::string_view StrFromMode(Mode mode);
	reindexer::Error FromJSON(std::span<char> json);
	void GetJSON(reindexer::WrSerializer& ser) const;
};

}  // namespace reindexer_tool
