#pragma once

#include <vector>

namespace reindexer {

enum [[nodiscard]] TagValues : int {
	StartObject = -1,
	EndObject = -2,
	StartArrayItem = -3,
	EndArrayItem = -4,
};

static const int KUnknownFieldSize = -1;
static const int kStandardFieldSize = 1;

using TagsLengths = std::vector<int>;
int computeObjectLength(TagsLengths& tagsLengths, std::size_t startTag, std::size_t& nextPos);

}  // namespace reindexer
