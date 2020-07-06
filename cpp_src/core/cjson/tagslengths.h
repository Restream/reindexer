#pragma once

#include "estl/h_vector.h"

namespace reindexer {

enum TagValues : int {
	StartObject = -1,
	EndObject = -2,
	StartArrayItem = -3,
	EndArrayItem = -4,
};

static const int KUnknownFieldSize = -1;
static const int kStandardFieldSize = 1;

using TagsLengths = h_vector<int, 0>;
int computeObjectLength(TagsLengths& tagsLengths, size_t startTag, size_t& nextPos);

}  // namespace reindexer
