#pragma once

#include "estl/h_vector.h"
#include "tagsmatcher.h"

using reindexer::h_vector;
using reindexer::TagsMatcher;

namespace reindexer {

class JsonPrintFilter {
public:
	JsonPrintFilter() {}
	JsonPrintFilter(const TagsMatcher &tagsMatcher, const h_vector<string, 4> &filter) {
		for (auto &str : filter) {
			int tag = tagsMatcher.name2tag(str.c_str());
			if (!filter_.size()) filter_.push_back(true);
			if (tag) {
				if (tag >= int(filter_.size())) {
					filter_.insert(filter_.end(), 1 + tag - filter_.size(), false);
				}
				filter_[tag] = true;
			}
		}
	}

	bool Match(int tag) const { return filter_.empty() || (tag < int(filter_.size()) && filter_[tag]); }

protected:
	h_vector<uint8_t, 32> filter_;
};

}  // namespace reindexer
