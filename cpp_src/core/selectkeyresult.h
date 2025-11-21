#pragma once

#include <climits>

#include "core/idset.h"
#include "core/index/indexiterator.h"
#include "core/nsselecter/comparator/comparator_indexed.h"
#include "core/nsselecter/comparator/comparator_not_indexed.h"

namespace reindexer {

/// Stores result of selecting data for only 1 value of
/// a certain key. i.e. for condition "A>=10 && A<20" it
/// contains only 1 IdSet for one of the following keys:
/// 10, 11, 12, 13, ... 19 (For example all rowIds for
/// the value '10').
class [[nodiscard]] SingleSelectKeyResult {
	friend class SelectIterator;
	friend class SelectKeyResult;

public:
	SingleSelectKeyResult() noexcept {}
	explicit SingleSelectKeyResult(IndexIterator::Ptr&& indexForwardIter) noexcept : indexForwardIter_(std::move(indexForwardIter)) {
		assertrx(indexForwardIter_ != nullptr);
	}
	template <typename KeyEntryT>
	explicit SingleSelectKeyResult(const KeyEntryT& ids, SortType sortId) noexcept {
		if (ids.Unsorted().IsCommitted()) {
			ids_ = ids.Sorted(sortId);
		} else {
			assertrx(ids.Unsorted().BTree());
			assertrx(!sortId);
			set_ = ids.Unsorted().BTree();
			useBtree_ = true;
		}
	}
	explicit SingleSelectKeyResult(IdSet::Ptr&& ids) noexcept : tempIds_(std::move(ids)), ids_(*tempIds_) {}
	explicit SingleSelectKeyResult(IdSetCRef ids) noexcept : ids_(ids) {}
	explicit SingleSelectKeyResult(IdType rBegin, IdType rEnd) noexcept : rBegin_(rBegin), rEnd_(rEnd), isRange_(true) {}
	SingleSelectKeyResult(const SingleSelectKeyResult& other) noexcept
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
	SingleSelectKeyResult& operator=(const SingleSelectKeyResult& other) noexcept {
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
	IdSetCRef ids_;

protected:
	const base_idsetset* set_ = nullptr;

	union {
		IdSetCRef::iterator begin_;
		IdSetCRef::reverse_iterator rbegin_;
		base_idsetset::const_iterator setbegin_;
		base_idsetset::const_reverse_iterator setrbegin_;
		int rBegin_ = 0;
		int rrBegin_;
	};

	union {
		IdSetCRef::iterator end_;
		IdSetCRef::reverse_iterator rend_;
		base_idsetset::const_iterator setend_;
		base_idsetset::const_reverse_iterator setrend_;
		int rEnd_ = 0;
		int rrEnd_;
	};

	union {
		IdSetCRef::iterator it_;
		IdSetCRef::reverse_iterator rit_;
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
class [[nodiscard]] SelectKeyResult : public h_vector<SingleSelectKeyResult, 1> {
public:
	constexpr static size_t kMinSetsForHeapSort = 16;
	constexpr static size_t kSelectionSortIdsCount = 500;
	constexpr static size_t kMinSetsForGenericSort = 30;

	struct [[nodiscard]] MergeOptions {
		bool genericSort;
		bool shrinkResult;
	};

	bool deferedExplicitSort = false;

	static size_t GetMergeSortCost(size_t idsCount, size_t idsetsCount) noexcept { return idsCount * idsetsCount; }
	static size_t GetGenericSortCost(size_t idsCount) noexcept { return idsCount * log2(idsCount) + 2 * idsCount; }
	static bool IsGenericSortRecommended(size_t idsetsCount, size_t idsCount, size_t maxIterations) noexcept {
		return idsetsCount >= kMinSetsForGenericSort && idsCount &&
			   GetGenericSortCost(idsCount) < GetMergeSortCost(maxIterations, idsetsCount);
	}
	static size_t CostWithDefferedSort(size_t idsetsCount, size_t idsCount, size_t maxIterations) noexcept {
		const auto mrgSortCost = GetMergeSortCost(maxIterations, idsetsCount);
		if (idsetsCount < kMinSetsForGenericSort || !idsCount) {
			return mrgSortCost;
		}
		const auto genSortCost = GetGenericSortCost(idsCount);
		return std::min(genSortCost, mrgSortCost);
	}

	/// Returns total amount of rowIds in all
	/// the SingleSelectKeyResult objects, i.e.
	/// maximum amount of possible iterations.
	/// @return amount of loops.
	size_t GetMaxIterations(size_t limitIters = std::numeric_limits<size_t>::max()) const noexcept {
		size_t cnt = 0;
		for (const SingleSelectKeyResult& r : *this) {
			if (r.indexForwardIter_) {
				auto iters = r.indexForwardIter_->GetMaxIterations(limitIters);
				if (iters == std::numeric_limits<size_t>::max()) {
					return limitIters;
				}
				cnt += iters;
			} else if (r.isRange_) {
				cnt += std::abs(r.rEnd_ - r.rBegin_);
			} else if (r.useBtree_) {
				cnt += r.set_->size();
			} else {
				cnt += r.ids_.size();
			}
			if (cnt > limitIters) {
				return limitIters;
			}
		}
		return cnt;
	}

	/// Represents data as one sorted set.
	/// Creates 1 set from all the inner
	/// SingleSelectKeyResult objects. Such
	/// representation makes further work with
	/// the object much easier.
	/// @param opts - merge customization options
	/// @return Pointer to a sorted IdSet object made
	/// from all the SingleSelectKeyResult inner objects.
	IdSet::Ptr MergeIdsets(MergeOptions&& opts, size_t idsCount) {
		IdSet::Ptr mergedIds;
		if (opts.genericSort) {
			mergedIds = mergeGenericSort(idsCount);
		} else if (idsCount < kSelectionSortIdsCount || size() < kMinSetsForHeapSort) {
			mergedIds = mergeSelectionSort(idsCount);
		} else {
			mergedIds = mergeHeapSort(idsCount);
		}
		if (opts.shrinkResult) {
			mergedIds->shrink_to_fit();
		}
		clear();
		deferedExplicitSort = false;
		emplace_back(IdSet::Ptr(mergedIds));
		return mergedIds;
	}

private:
	IdSet::Ptr mergeGenericSort(size_t idsCount) {
		base_idset ids;
		size_t actualSize = 0;
		ids.resize(idsCount);
		auto rit = ids.begin();
		for (auto it = begin(), endIt = end(); it != endIt; ++it) {
			if (it->isRange_) [[unlikely]] {
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
		return IdSet::BuildFromUnsorted(std::move(ids));
	}

	IdSet::Ptr mergeSelectionSort(size_t idsCount) {
		auto mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
		mergedIds->reserve(idsCount);

		auto firstSetIt = std::partition(begin(), end(), [](const SingleSelectKeyResult& v) noexcept { return !v.useBtree_; });
		const auto vecsCnt = firstSetIt - begin();

		h_vector<value_type*, 64> ptrsVec;
		ptrsVec.reserve(size());

		for (auto& v : *this) {
			if (v.isRange_) [[unlikely]] {
				throw Error(errLogic, "Unable to merge 'range' idset ('merge sort mode')");
			}
			ptrsVec.emplace_back(&v);
		}
		std::span<value_type*> vecSpan(ptrsVec.data(), vecsCnt);
		std::span<value_type*> setSpan(ptrsVec.data() + vecsCnt, size() - vecsCnt);

		for (auto& v : vecSpan) {
			assertrx_dbg(!v->useBtree_);
			v->it_ = v->ids_.begin();
			v->end_ = v->ids_.end();
		}
		for (auto& v : setSpan) {
			assertrx_dbg(v->useBtree_);
			v->itset_ = v->set_->begin();
			v->setend_ = v->set_->end();
		}

		int min = INT_MIN;
		for (;;) {
			int curMin = INT_MAX;
			for (auto vsIt = vecSpan.begin(), vsItEnd = vecSpan.end(); vsIt != vsItEnd;) {
				auto& itvec = (*vsIt)->it_;
				auto& vecend = (*vsIt)->end_;
				for (;; ++itvec) {
					if (itvec == vecend) {
						std::swap(*vsIt, vecSpan.back());
						vecSpan = std::span<value_type*>(vecSpan.data(), vecSpan.size() - 1);
						--vsItEnd;
						break;
					}
					const auto val = *itvec;
					if (val > min) {
						if (val < curMin) {
							curMin = val;
						}
						++vsIt;
						break;
					}
				}
			}
			for (auto ssIt = setSpan.begin(), ssItEnd = setSpan.end(); ssIt != ssItEnd;) {
				auto& itset = (*ssIt)->itset_;
				auto& setend = (*ssIt)->setend_;
				for (;; ++itset) {
					if (itset == setend) {
						std::swap(*ssIt, setSpan.back());
						setSpan = std::span<value_type*>(setSpan.data(), setSpan.size() - 1);
						--ssItEnd;
						break;
					}
					const auto val = *itset;
					if (val > min) {
						if (val < curMin) {
							curMin = val;
						}
						++ssIt;
						break;
					}
				}
			}
			if (curMin == INT_MAX) {
				break;
			}
			min = curMin;
			mergedIds->AddUnordered(min);
		}
		return mergedIds;
	}

	IdSet::Ptr mergeHeapSort(size_t idsCount) {
		auto mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
		mergedIds->reserve(idsCount);

		struct [[nodiscard]] IdSetGreater {
			bool operator()(const value_type* l, const value_type* r) noexcept {
				const auto lval = l->useBtree_ ? *(l->itset_) : *(l->it_);
				const auto rval = r->useBtree_ ? *(r->itset_) : *(r->it_);
				return lval > rval;
			}
		};

		h_vector<value_type*, 64> ptrsVec;
		ptrsVec.reserve(size());
		for (auto& v : *this) {
			if (v.isRange_) [[unlikely]] {
				throw Error(errLogic, "Unable to merge 'range' idset ('merge sort mode')");
			}

			if (v.useBtree_) {
				if (!v.set_->empty()) {
					ptrsVec.emplace_back(&v);
					v.itset_ = v.set_->begin();
					v.setend_ = v.set_->end();
				}
			} else {
				if (!v.ids_.empty()) {
					ptrsVec.emplace_back(&v);
					v.it_ = v.ids_.begin();
					v.end_ = v.ids_.end();
				}
			}
		}
		std::span<value_type*> idsetsSpan(ptrsVec.data(), ptrsVec.size());
		std::make_heap(idsetsSpan.begin(), idsetsSpan.end(), IdSetGreater{});
		int min = INT_MIN;
		auto handleMinValue = [&mergedIds, &idsetsSpan, &min](auto& it, auto end) {
			auto val = *it;
			if (val > min) {
				mergedIds->AddUnordered(val);
				min = val;
			}
			do {
				if (++it == end) {
					std::swap(idsetsSpan.front(), idsetsSpan.back());
					idsetsSpan = std::span<value_type*>(idsetsSpan.begin(), idsetsSpan.size() - 1);
					return;
				}
			} while (*it <= min);
		};

		while (!idsetsSpan.empty()) {
			auto& minV = *idsetsSpan.front();
			if (minV.useBtree_) {
				handleMinValue(minV.itset_, minV.setend_);
			} else {
				handleMinValue(minV.it_, minV.end_);
			}
			heapifyRoot<value_type*, IdSetGreater>(idsetsSpan);
		}
		return mergedIds;
	}

	template <typename T, typename CompareT>
	RX_ALWAYS_INLINE void heapifyRoot(std::span<T> vec) noexcept {
		static_assert(std::is_pointer_v<T>, "Expecting T being a pointer for the fast swaps");
		T* target = vec.data();
		T* end = target + vec.size();
		CompareT c;
		for (size_t i = 0;;) {
			T* cur = target;
			const auto lIdx = (i << 1) + 1;
			T* left = vec.data() + lIdx;
			T* right = left + 1;

			if (left < end && c(*target, *left)) {
				target = left;
				i = lIdx;
			}
			if (right < end && c(*target, *right)) {
				target = right;
				i = lIdx + 1;
			}
			if (cur == target) {
				return;
			}
			std::swap(*cur, *target);
		}
	}
};

using SelectKeyResultsVector = h_vector<SelectKeyResult, 1>;

/// Result of selecting data for
/// each key in a query.
class SelectKeyResults : public std::variant<SelectKeyResultsVector, ComparatorNotIndexed, ComparatorIndexed<bool>, ComparatorIndexed<int>,
											 ComparatorIndexed<int64_t>, ComparatorIndexed<double>, ComparatorIndexed<key_string>,
											 ComparatorIndexed<PayloadValue>, ComparatorIndexed<Point>, ComparatorIndexed<Uuid>,
											 ComparatorIndexed<FloatVector>> {
	using Base =
		std::variant<SelectKeyResultsVector, ComparatorNotIndexed, ComparatorIndexed<bool>, ComparatorIndexed<int>,
					 ComparatorIndexed<int64_t>, ComparatorIndexed<double>, ComparatorIndexed<key_string>, ComparatorIndexed<PayloadValue>,
					 ComparatorIndexed<Point>, ComparatorIndexed<Uuid>, ComparatorIndexed<FloatVector>>;

public:
	SelectKeyResults() noexcept : Base{SelectKeyResultsVector{}} {}
	SelectKeyResults(SelectKeyResult&& res) noexcept : Base{SelectKeyResultsVector{std::move(res)}} {}
	template <typename T>
	SelectKeyResults(ComparatorIndexed<T>&& comp) noexcept : Base{std::move(comp)} {}
	SelectKeyResults(ComparatorNotIndexed&& comp) noexcept : Base{std::move(comp)} {}
	void Clear() noexcept { std::get<SelectKeyResultsVector>(*this).clear(); }
	void EmplaceBack(SelectKeyResult&& sr) { std::get<SelectKeyResultsVector>(*this).emplace_back(std::move(sr)); }
	[[nodiscard]] bool IsComparator() const noexcept { return !std::holds_alternative<SelectKeyResultsVector>(AsVariant()); }
	SelectKeyResult&& Front() && noexcept { return std::move(std::get<SelectKeyResultsVector>(*this).front()); }
	const Base& AsVariant() const& noexcept { return *this; }
	Base& AsVariant() & noexcept { return *this; }
	auto AsVariant() const&& = delete;
};

}  // namespace reindexer
