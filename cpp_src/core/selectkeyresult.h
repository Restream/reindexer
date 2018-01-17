#pragma once

#include <climits>
#include <memory>

#include "core/comparator.h"
#include "core/ft/fulltextctx.h"
#include "core/idset.h"
namespace reindexer {

class SingleSelectKeyResult {
	friend class SelectIterator;
	friend class SelectKeyResult;

public:
	SingleSelectKeyResult() {}
	SingleSelectKeyResult(const IdSetRef &ids) : ids_(ids), isRange_(false) {}
	SingleSelectKeyResult(IdSet::Ptr ids) : tempIds_(ids), ids_(ids.get()), isRange_(false) {}
	SingleSelectKeyResult(IdType rBegin, IdType rEnd) : rBegin_(rBegin), rEnd_(rEnd), isRange_(true) {}

protected:
	IdSet::Ptr tempIds_;
	IdSetRef ids_;

	union {
		IdSetRef::const_iterator begin_;
		IdSetRef::const_reverse_iterator rbegin_;
		int rBegin_;
		int rrBegin_;
	};

	union {
		IdSetRef::const_iterator end_;
		IdSetRef::const_reverse_iterator rend_;
		int rEnd_;
		int rrEnd_;
	};

	union {
		IdSetRef::const_iterator it_;
		IdSetRef::const_reverse_iterator rit_;
		int rIt_;
		int rrIt_;
	};

	bool bsearch_ = false;
	bool isRange_;
};

class SelectKeyResult : public h_vector<SingleSelectKeyResult, 1> {
public:
	h_vector<Comparator, 1> comparators_;

	IdSet::Ptr mergeIdsets() {
		auto mergedIds = std::make_shared<IdSet>();

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
			mergedIds->Add(curMin, IdSet::Unordered);
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
	SelectKeyResults() {}
	FullTextCtx::Ptr ctx;
};

}  // namespace reindexer
