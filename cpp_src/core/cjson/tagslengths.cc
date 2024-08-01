#include "tagslengths.h"

namespace reindexer {

int computeObjectLength(TagsLengths& tagsLengths, std::size_t startTag, std::size_t& nextPos) {
	bool arrayItem = false;
	std::size_t i = startTag;
	int objectLength = 0;
	while (i < tagsLengths.size()) {
		if (tagsLengths[i] == TagValues::StartObject) {
			std::size_t pos;
			tagsLengths[i] = computeObjectLength(tagsLengths, i + 1, pos);
			if (!arrayItem) {
				++objectLength;
			}
			i = pos;
		} else if (tagsLengths[i] == TagValues::EndObject) {
			++i;
			break;
		} else if (tagsLengths[i] == TagValues::StartArrayItem) {
			arrayItem = true;
			++i;
		} else if (tagsLengths[i] == TagValues::EndArrayItem) {
			arrayItem = false;
			++i;
		} else {
			if (!arrayItem) {
				++objectLength;
			}
			++i;
		}
	}
	nextPos = i;
	return objectLength;
}

}  // namespace reindexer
