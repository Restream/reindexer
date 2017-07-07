#pragma once

#include <climits>
#include <memory>
#include <unordered_map>
#include "algoritm/full_text_search/dataholder/datastruct.h"
#include "core/comparator.h"
#include "core/idset.h"

namespace reindexer {
using std::shared_ptr;
using std::vector;
using std::unordered_map;

class SingleSelectKeyResult {
	friend class QueryIterator;
	friend class SelectKeyResult;

public:
	SingleSelectKeyResult(){};
	SingleSelectKeyResult(const IdSetRef &ids) : ids_(ids), isRange_(false) {}
	SingleSelectKeyResult(IdSet::Ptr ids) : tempIds_(ids), ids_(ids.get()), isRange_(false) {}
	SingleSelectKeyResult(IdType rBegin, IdType rEnd)
		: isRange_(true), rBegin_(rBegin), rEnd_(rEnd), rrBegin_(rEnd - 1), rrEnd_(rBegin - 1) {}

protected:
	shared_ptr<IdSet> tempIds_;
	IdSetRef ids_;
	IdSetRef::const_iterator begin_ = nullptr, end_ = nullptr, it_ = nullptr;

	// Reverse iterators for desc sort
	IdSetRef::const_reverse_iterator rbegin_, rend_, rit_;

	bool isRange_;
	int rBegin_, rEnd_, rIt_ = 0;
	int rrBegin_, rrEnd_, rrIt_ = 0;
	bool bsearch_ = false;
};

class SelectKeyResult : public h_vector<SingleSelectKeyResult, 1> {
public:
	vector<Comparator> comparators_;

	shared_ptr<IdSet> mergeIdsets() {
		auto mergedIds = make_shared<IdSet>();

		size_t expectSize = 0;
		for (auto it = begin(); it != end(); it++) {
			it->it_ = it->ids_.begin();
			expectSize += it->ids_.size();
		}
		mergedIds->reserve(expectSize);

		for (;;) {
			int min = mergedIds->size() ? mergedIds->back() : INT_MIN;
			int curMin = INT_MAX;
			for (auto it = begin(); it != end(); it++) {
				for (; it->it_ != it->ids_.end() && *it->it_ <= min; it->it_++) {
				};
				if (it->it_ != it->ids_.end() && *it->it_ < curMin) curMin = *it->it_;
			}
			if (curMin == INT_MAX) break;
			mergedIds->push_back(curMin);
		};
		mergedIds->shrink_to_fit();
		clear();
		push_back(SingleSelectKeyResult(mergedIds));
		return mergedIds;
	}
};

class SelectKeyResults : public h_vector<SelectKeyResult, 1> {
public:
	SelectKeyResults(std::initializer_list<SelectKeyResult> l) { insert(end(), l.begin(), l.end()); }
	SelectKeyResults(const SelectKeyResult &res) { push_back(res); }
	SelectKeyResults(){};
};

}  // namespace reindexer