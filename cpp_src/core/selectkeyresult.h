#pragma once

#include <climits>
#include <memory>

#include "core/comparator.h"
#include "core/idset.h"

namespace reindexer {

class SingleSelectKeyResult {
	friend class SelectIterator;
	friend class SelectKeyResult;

public:
	SingleSelectKeyResult() {}
	explicit SingleSelectKeyResult(const IdSetRef &ids) : ids_(ids), isRange_(false) {}
	explicit SingleSelectKeyResult(IdSet::Ptr ids) : tempIds_(ids), ids_(ids.get()), isRange_(false) {}
	explicit SingleSelectKeyResult(IdType rBegin, IdType rEnd) : rBegin_(rBegin), rEnd_(rEnd), isRange_(true) {}
	SingleSelectKeyResult(const SingleSelectKeyResult &other)
		: tempIds_(other.tempIds_), ids_(other.ids_), bsearch_(other.bsearch_), isRange_(other.isRange_) {
		if (isRange_) {
			rBegin_ = other.rBegin_;
			rEnd_ = other.rEnd_;
			rIt_ = other.rIt_;
		} else {
			begin_ = other.begin_;
			end_ = other.end_;
			it_ = other.it_;
		}
	}
	SingleSelectKeyResult &operator=(const SingleSelectKeyResult &other) {
		if (&other != this) {
			tempIds_ = other.tempIds_;
			ids_ = other.ids_;
			bsearch_ = other.bsearch_;
			isRange_ = other.isRange_;
			if (isRange_) {
				rBegin_ = other.rBegin_;
				rEnd_ = other.rEnd_;
				rIt_ = other.rIt_;
			} else {
				begin_ = other.begin_;
				end_ = other.end_;
				it_ = other.it_;
			}
		}
		return *this;
	}

protected:
	IdSet::Ptr tempIds_;
	IdSetRef ids_;

	union {
		IdSetRef::const_iterator begin_;
		IdSetRef::const_reverse_iterator rbegin_;
		int rBegin_ = 0;
		int rrBegin_;
	};

	union {
		IdSetRef::const_iterator end_;
		IdSetRef::const_reverse_iterator rend_;
		int rEnd_ = 0;
		int rrEnd_;
	};

	union {
		IdSetRef::const_iterator it_;
		IdSetRef::const_reverse_iterator rit_;
		int rIt_ = 0;
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
};

}  // namespace reindexer
