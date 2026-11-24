#pragma once

#include <span>
#include <string>
#include <string_view>
#include "core/embedding/protocol/iembed_protocol.h"

namespace reindexer::embedding {

void BuildQueryView(std::string_view text, std::string& out);
void BuildUpsertView(std::span<const DocSource> sources, std::string& out);
bool MatchHttpUrl(std::string_view url);

}  // namespace reindexer::embedding
