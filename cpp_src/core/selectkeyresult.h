#pragma once

#include <climits>
#include <memory>

#include "core/comparator.h"
#include "core/idset.h"
#include "core/index/indexiterator.h"
#include "index/keyentry.h"

namespace reindexer {

/// Stores result of selecting data for only 1 value of
/// a certain key. i.e. for condition "A>=10 && A<20" it
/// contains only 1 IdSet for one of the following keys:
/// 10, 11, 12, 13, ... 19 (For example all rowIds for
/// the value '10').
class SingleSelectKeyResult {
	friend class SelectIterator;
	friend class SelectKeyResult;

public:
	SingleSelectKeyResult() noexcept {}
	explicit SingleSelectKeyResult(IndexIterator::Ptr &&indexForwardIter) noexcept : indexForwardIter_(std::move(indexForwardIter)) {
		assertrx(indexForwardIter_ != nullptr);
	}
	template <typename KeyEntryT>
	explicit SingleSelectKeyResult(const KeyEntryT &ids, SortType sortId) noexcept {
		if (ids.Unsorted().IsCommited()) {
			ids_ = ids.Sorted(sortId);
		} else {
			assertrx(ids.Unsorted().BTree());
			assertrx(!sortId);
			set_ = ids.Unsorted().BTree();
			useBtree_ = true;
		}
	}
	explicit SingleSelectKeyResult(IdSet::Ptr &&ids) noexcept : tempIds_(std::move(ids)), ids_(*tempIds_) {}
	explicit SingleSelectKeyResult(const IdSetRef &ids) noexcept : ids_(ids) {}
	explicit SingleSelectKeyResult(IdType rBegin, IdType rEnd) noexcept : rBegin_(rBegin), rEnd_(rEnd), isRange_(true) {}
	SingleSelectKeyResult(const SingleSelectKeyResult &other) noexcept
		: tempIds_(other.tempIds_),
		  ids_(other.ids_),
		  set_(other.set_),
		  indexForwardIter_(other.indexForwardIter_),
		  bsearch_(other.bsearch_),
		  isRange_(other.isRange_),
		  useBtree_(other.useBtree_) {
		if (isRange_) {
			rBegin_ = other.rBegin_;
			rEnd_ = other.rEnd_;
			rIt_ = other.rIt_;
		} else {
			if (useBtree_) {
				setbegin_ = other.setbegin_;
				setrbegin_ = other.setrbegin_;
				setend_ = other.setend_;
				setrend_ = other.setrend_;
				itset_ = other.itset_;
				ritset_ = other.ritset_;
			} else {
				begin_ = other.begin_;
				end_ = other.end_;
				it_ = other.it_;
			}
		}
	}
	SingleSelectKeyResult &operator=(const SingleSelectKeyResult &other) noexcept {
		if (&other != this) {
			tempIds_ = other.tempIds_;
			ids_ = other.ids_;
			set_ = other.set_;
			indexForwardIter_ = other.indexForwardIter_;
			bsearch_ = other.bsearch_;
			isRange_ = other.isRange_;
			useBtree_ = other.useBtree_;
			if (isRange_) {
				rBegin_ = other.rBegin_;
				rEnd_ = other.rEnd_;
				rIt_ = other.rIt_;
			} else {
				if (useBtree_) {
					setbegin_ = other.setbegin_;
					setrbegin_ = other.setrbegin_;
					setend_ = other.setend_;
					setrend_ = other.setrend_;
					itset_ = other.itset_;
					ritset_ = other.ritset_;
				} else {
					begin_ = other.begin_;
					end_ = other.end_;
					it_ = other.it_;
				}
			}
		}
		return *this;
	}

	IdSet::Ptr tempIds_;
	IdSetRef ids_;

protected:
	const base_idsetset *set_ = nullptr;

	union {
		IdSetRef::const_iterator begin_;
		IdSetRef::const_reverse_iterator rbegin_;
		base_idsetset::const_iterator setbegin_;
		base_idsetset::const_reverse_iterator setrbegin_;
		int rBegin_ = 0;
		int rrBegin_;
	};

	union {
		IdSetRef::const_iterator end_;
		IdSetRef::const_reverse_iterator rend_;
		base_idsetset::const_iterator setend_;
		base_idsetset::const_reverse_iterator setrend_;
		int rEnd_ = 0;
		int rrEnd_;
	};

	union {
		IdSetRef::const_iterator it_;
		IdSetRef::const_reverse_iterator rit_;
		base_idsetset::const_iterator itset_;
		base_idsetset::const_reverse_iterator ritset_;
		int rIt_ = 0;
		int rrIt_;
	};

	IndexIterator::Ptr indexForwardIter_;

	// if isRange is true then bsearch is always false
	bool bsearch_ = false;
	bool isRange_ = false;
	bool useBtree_ = false;
};

/// Stores results of selecting data for 1 certain key,
/// i.e. for condition "A>=10 && A<20" there will be
/// 10 SingleSelectKeyResult objects (for each of the
/// following keys: 10, 11, 12, 13, ... 19).
class SelectKeyResult : public h_vector<SingleSelectKeyResult, 1> {
public:
	std::vector<Comparator> comparators_;
	bool deferedExplicitSort = false;

	static size_t GetMergeSortCost(size_t idsCount, size_t idsetsCount) noexcept { return idsCount * idsetsCount; }
	static size_t GetGenericSortCost(size_t idsCount) noexcept { return idsCount * log2(idsCount) + 2 * idsCount; }
	static bool IsGenericSortRecommended(size_t idsetsCount, size_t idsCount, size_t maxIterations) noexcept {
		return idsetsCount >= 30 && idsCount && GetGenericSortCost(idsCount) < GetMergeSortCost(maxIterations, idsetsCount);
	}
	static size_t CostWithDefferedSort(size_t idsetsCount, size_t idsCount, size_t maxIterations) noexcept {
		const auto genSortCost = GetGenericSortCost(idsCount);
		if (idsetsCount < 30 || !idsCount) {
			return genSortCost;
		}
		const auto mrgSortCost = GetMergeSortCost(maxIterations, idsetsCount);
		return std::min(genSortCost, mrgSortCost);
	}

	void ClearDistinct() {
		for (Comparator &comp : comparators_) comp.ClearDistinct();
	}
	/// Returns total amount of rowIds in all
	/// the SingleSelectKeyResult objects, i.e.
	/// maximum amonut of possible iterations.
	/// @return amount of loops.
	size_t GetMaxIterations(size_t limitIters = std::numeric_limits<size_t>::max()) const noexcept {
		size_t cnt = 0;
		for (const SingleSelectKeyResult &r : *this) {
			if (r.indexForwardIter_) {
				cnt += r.indexForwardIter_->GetMaxIterations(limitIters);
			} else if (r.isRange_) {
				cnt += std::abs(r.rEnd_ - r.rBegin_);
			} else if (r.useBtree_) {
				cnt += r.set_->size();
			} else {
				cnt += r.ids_.size();
			}
			if (cnt > limitIters) break;
		}
		return cnt;
	}

	/// Represents data as one sorted set.
	/// Creates 1 set from all the inner
	/// SingleSelectKeyResult objects. Such
	/// representation makes further work with
	/// the object much easier.
	/// @param useGenericSort - use generic sort instead of merge sort
	/// @return Pointer to a sorted IdSet object made
	/// from all the SingleSelectKeyResult inner objects.
	IdSet::Ptr MergeIdsets(bool useGenericSort, size_t idsCount) {
		IdSet::Ptr mergedIds;
		if (useGenericSort) {
			base_idset ids;
			size_t actualSize = 0;
			ids.resize(idsCount);
			auto rit = ids.begin();
			for (auto it = begin(), endIt = end(); it != endIt; ++it) {
				if (it->isRange_) {
					throw Error(errLogic, "Unable to merge 'range' idset ('generic sort mode')");
				}
				if (it->useBtree_) {
					const auto sz = it->set_->size();
					actualSize += sz;
					std::copy(it->set_->begin(), it->set_->end(), rit);
					rit += sz;
				} else {
					const auto sz = it->ids_.size();
					actualSize += sz;
					std::copy(it->ids_.begin(), it->ids_.end(), rit);
					rit += sz;
				}
			}
			assertrx(idsCount == actualSize);
			mergedIds = IdSet::BuildFromUnsorted(std::move(ids));
		} else {
			mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
			mergedIds->reserve(idsCount);
			for (auto it = begin(), endIt = end(); it != endIt; ++it) {
				if (it->isRange_) {
					throw Error(errLogic, "Unable to merge 'range' idset ('merge sort mode')");
				}
				if (it->useBtree_) {
					it->itset_ = it->set_->begin();
				} else {
					it->it_ = it->ids_.begin();
				}
			}
			for (;;) {
				const int min = mergedIds->size() ? mergedIds->back() : INT_MIN;
				int curMin = INT_MAX;
				for (auto it = begin(), endIt = end(); it != endIt; ++it) {
					if (it->useBtree_) {
						const auto end = it->set_->end();
						for (; it->itset_ != end && *it->itset_ <= min; ++it->itset_) {
						}
						if (it->itset_ != end && *it->itset_ < curMin) curMin = *it->itset_;
					} else {
						const auto end = it->ids_.end();
						for (; it->it_ != end && *it->it_ <= min; ++it->it_) {
						}
						if (it->it_ != end && *it->it_ < curMin) curMin = *it->it_;
					}
				}
				if (curMin == INT_MAX) break;
				mergedIds->Add(curMin, IdSet::Unordered, 0);
			}
			mergedIds->shrink_to_fit();
		}
		clear();
		deferedExplicitSort = false;
		emplace_back(IdSet::Ptr(mergedIds));
		return mergedIds;
	}
};

/// Result of selecting data for
/// each key in a query.
class SelectKeyResults : public h_vector<SelectKeyResult, 1> {
public:
	SelectKeyResults(std::initializer_list<SelectKeyResult> l) { insert(end(), l.begin(), l.end()); }
	SelectKeyResults(SelectKeyResult &&res) { push_back(std::move(res)); }
	SelectKeyResults() = default;
};

}  // namespace reindexer
