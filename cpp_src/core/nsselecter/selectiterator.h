#pragma once

#include "core/enums.h"
#include "core/selectkeyresult.h"
#include "estl/concepts.h"

namespace reindexer {

/// Allows to iterate over a result of selecting
/// data for one certain key.
class [[nodiscard]] SelectIterator : public SelectKeyResult {
public:
	enum {
		Forward,
		Reverse,
		SingleRange,
		SingleIdset,
		SingleIdSetWithDeferedSort,
		RevSingleRange,
		RevSingleIdset,
		RevSingleIdSetWithDeferedSort,
		Unsorted,
		UnbuiltSortOrdersIndex,
	};

	template <concepts::ConvertibleToString Str>
	SelectIterator(SelectKeyResult&& res, reindexer::IsDistinct distinct, Str&& n, int indexNo,
				   ForcedFirst forcedFirst = ForcedFirst_False) noexcept
		: SelectKeyResult(std::move(res)),
		  name(std::forward<Str>(n)),
		  type_(Forward),
		  forcedFirst_(forcedFirst),
		  indexNo_(indexNo),
		  distinct_(distinct) {}

	/// Starts iteration process: prepares
	/// object for further work.
	/// @param reverse - direction of iteration.
	/// @param maxIterations - expected max iterations in select loop
	void Start(bool reverse, int maxIterations) {
		const bool explicitSort = applyDeferedSort(maxIterations);

		isReverse_ = reverse;
		const auto begIt = begin();
		lastPos_ = 0;

		for (auto it = begIt, endIt = end(); it != endIt; ++it) {
			if (it->isRange_) {
				if (isReverse_) {
					const auto rrBegin = it->rEnd_ - 1;
					it->rrEnd_ = it->rBegin_ - 1;
					it->rrBegin_ = rrBegin;
					it->rrIt_ = rrBegin;
				} else {
					it->rIt_ = it->rBegin_;
				}
			} else {
				if (it->useBtree_) {
					assertrx_dbg(it->set_);
					if (reverse) {
						const auto setRBegin = it->set_->rbegin();
						it->ritset_ = setRBegin;
						it->setrbegin_ = setRBegin;
						it->setrend_ = it->set_->rend();
					} else {
						const auto setBegin = it->set_->begin();
						it->itset_ = setBegin;
						it->setbegin_ = setBegin;
						it->setend_ = it->set_->end();
					}
				} else {
					if (isReverse_) {
						const auto idsRBegin = it->ids_.rbegin();
						it->rend_ = it->ids_.rend();
						it->rit_ = idsRBegin;
						it->rbegin_ = idsRBegin;
					} else {
						const auto idsBegin = it->ids_.begin();
						it->end_ = it->ids_.end();
						it->it_ = idsBegin;
						it->begin_ = idsBegin;
					}
				}
			}
		}

		lastVal_ = isReverse_ ? INT_MAX : INT_MIN;
		if (isUnsorted_) {
			type_ = Unsorted;
		} else if (size() == 1) {
			if (begIt->indexForwardIter_) {
				type_ = UnbuiltSortOrdersIndex;
				begIt->indexForwardIter_->Start(reverse);
			} else if (!isReverse_) {
				type_ = begIt->isRange_ ? SingleRange : (explicitSort ? SingleIdSetWithDeferedSort : SingleIdset);
			} else {
				type_ = begIt->isRange_ ? RevSingleRange : (explicitSort ? RevSingleIdSetWithDeferedSort : RevSingleIdset);
			}
		} else {
			type_ = isReverse_ ? Reverse : Forward;
		}
	}
	/// Signalizes if iteration is over.
	/// @return true if iteration is done.
	RX_ALWAYS_INLINE bool End() const noexcept { return lastVal_ == (isReverse_ ? INT_MIN : INT_MAX); }
	/// Iterates to a next item of result. Increments 'matched' and 'total' counters.
	/// @param minHint - rowId value to start from.
	/// @return true if operation succeed.
	RX_ALWAYS_INLINE bool Next(IdType minHint) noexcept {
		++totalCalls_;
		bool res = nextImpl(minHint);
		matchedCount_ += int(res);
		return res;
	}
	/// Checks if current iterator contains specified ID. Adjusts iterator if needed.
	/// Increments 'matched' and 'total' counters.
	/// @param targetId - rowId value to search.
	/// @return true if targetId was found.
	RX_ALWAYS_INLINE bool Compare(IdType targetId) noexcept {
		++totalCalls_;
		const auto val = Val();
		if (isReverse_) {
			if (val < targetId) {
				return false;
			}
		} else if (val > targetId) {
			return false;
		}

		if (val == targetId || (nextImpl(targetId) && targetId == Val())) {
			++matchedCount_;
			return true;
		}
		return false;
	}

	/// Sets Unsorted iteration mode
	RX_ALWAYS_INLINE void SetUnsorted() noexcept { isUnsorted_ = true; }

	/// Current rowId
	RX_ALWAYS_INLINE IdType Val() const noexcept { return lastVal_; }

	/// Current rowId index since the beginning
	/// of current SingleKeyValue object.
	int Pos() const noexcept {
		switch (type_) {
			case SingleIdset:
			case SingleIdSetWithDeferedSort: {
				const auto& it = *begin();
				assertrx_throw(!it.useBtree_);
				return it.it_ - it.begin_;
			}
			case Forward:
			case Reverse:
			case SingleRange:
			case RevSingleRange:
			case RevSingleIdset:
			case RevSingleIdSetWithDeferedSort:
			case Unsorted:
			case UnbuiltSortOrdersIndex:
			default:
				assertrx_throw(!operator[](lastPos_).useBtree_ && (type_ != UnbuiltSortOrdersIndex));
				return operator[](lastPos_).it_ - operator[](lastPos_).begin_ - 1;
		}
	}

	/// @return number of matching items
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}

	/// Excludes last set of ids from each result
	/// to remove duplicated keys
	void ExcludeLastSet(IdType rowId) {
		if (type_ == UnbuiltSortOrdersIndex) {
			auto fwdIter = begin()->indexForwardIter_;
			if (fwdIter->Value() == rowId) {
				fwdIter->ExcludeLastSet();
			}
		} else if (!End() && lastPos_ != size() && lastVal_ == rowId) {
			assertrx_throw(!operator[](lastPos_).isRange_);
			if (operator[](lastPos_).useBtree_) {
				operator[](lastPos_).itset_ = operator[](lastPos_).setend_;
				operator[](lastPos_).ritset_ = operator[](lastPos_).setrend_;
			} else {
				operator[](lastPos_).it_ = operator[](lastPos_).end_;
				operator[](lastPos_).rit_ = operator[](lastPos_).rend_;
			}
		}
	}

	/// Appends result to an existing set.
	/// @param other - results to add.
	void Append(SelectKeyResult&& other) {
		reserve(size() + other.size());
		for (auto& r : other) {
			emplace_back(std::move(r));
		}
		other.clear();
	}
	/// Cost value used for sorting: object with a smaller
	/// cost goes before others.
	double Cost(int expectedIterations) const noexcept {
		if (type_ == UnbuiltSortOrdersIndex) {
			return -1;
		}
		if (forcedFirst_) {
			return -static_cast<double>(GetMaxIterations());
		}
		double result{0.0};
		const auto sz = size();
		if (distinct_) {
			result += sz;
		} else if (type_ != SingleIdSetWithDeferedSort && type_ != RevSingleIdSetWithDeferedSort && !deferedExplicitSort) {
			result += static_cast<double>(GetMaxIterations()) * sz;
		} else {
			result += static_cast<double>(CostWithDefferedSort(sz, GetMaxIterations(), expectedIterations));
		}
		return isNotOperation_ ? expectedIterations + result : result;
	}

	void SetNotOperationFlag(bool isNotOperation) noexcept { isNotOperation_ = isNotOperation; }

	/// Switches SingleSelectKeyResult to btree search
	/// mode if it's more efficient than just comparing
	/// each object in sequence.
	void SetExpectMaxIterations(int expectedIterations) noexcept {
		if (expectedIterations == std::numeric_limits<int>::max()) {
			// FIXME: Remove this branch. In some cases (check issues #1495 and #1935) maxIterations will have incorrect value.
			// Previously we've been using int32 type to calculate 'itersbsearch', that led to integer overwhelming and
			// setting 'bsearch_' to 'true'.
			// Special case for 'std::numeric_limits<int>::max()' preserves this behavior until related issues do not fixed.
			for (SingleSelectKeyResult& r : *this) {
				r.bsearch_ = true;
			}
		} else {
			for (SingleSelectKeyResult& r : *this) {
				if (!r.isRange_ && r.ids_.size() > 8) {
					const int64_t itersloop = r.ids_.size();
					const int64_t itersbsearch = std::log2(r.ids_.size()) * int64_t(expectedIterations);
					r.bsearch_ = itersbsearch < itersloop;
				}
			}
		}
	}

	int Type() const noexcept { return type_; }

	std::string_view TypeName() const noexcept;
	std::string Dump() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return distinct_; }
	int IndexNo() const noexcept { return indexNo_; }

	std::string name;

private:
	/// Iterates to a next item of result.
	/// @param minHint - rowId value to start from.
	/// @return true if operation succeed.
	RX_ALWAYS_INLINE bool nextImpl(IdType minHint) noexcept {
		switch (type_) {
			case Forward:
				return nextFwd(minHint);
			case Reverse:
				return nextRev(minHint);
			case SingleRange:
				return nextFwdSingleRange(minHint);
			case SingleIdset:
			case SingleIdSetWithDeferedSort:
				return nextFwdSingleIdset(minHint);
			case RevSingleRange:
				return nextRevSingleRange(minHint);
			case RevSingleIdset:
			case RevSingleIdSetWithDeferedSort:
				return nextRevSingleIdset(minHint);
			case Unsorted:
				return nextUnsorted();
			case UnbuiltSortOrdersIndex:
				return nextUnbuiltSortOrders();
			default:
				return false;
		}
	}

	// Iterates to a next item of result
	// depending on iterator type starting
	// from minHint which is the least rowId.
	// Generic next implementation
	bool nextFwd(IdType minHint) noexcept {
		const auto lastVal = (minHint > lastVal_) ? (minHint - 1) : lastVal_;
		int minVal = INT_MAX;
		const auto beg = begin();
		for (auto it = beg, endIt = end(); it != endIt; ++it) {
			if (it->useBtree_) {
				if (it->itset_ != it->setend_) {
					it->itset_ = it->set_->upper_bound(lastVal);
					if (it->itset_ != it->setend_ && *it->itset_ < minVal) {
						minVal = *it->itset_;
						lastPos_ = it - beg;
					}
				}
			} else {
				if (it->isRange_ && it->rIt_ != it->rEnd_) {
					it->rIt_ = std::min(it->rEnd_, std::max(it->rIt_, lastVal + 1));

					if (it->rIt_ != it->rEnd_ && it->rIt_ < minVal) {
						minVal = it->rIt_;
						lastPos_ = it - beg;
					}

				} else if (!it->isRange_ && it->it_ != it->end_) {
					for (; it->it_ != it->end_ && *it->it_ <= lastVal; ++it->it_) {
					}
					if (it->it_ != it->end_ && *it->it_ < minVal) {
						minVal = *it->it_;
						lastPos_ = it - beg;
					}
				}
			}
		}
		lastVal_ = minVal;
		return lastVal_ != INT_MAX;
	}
	bool nextRev(IdType maxHint) noexcept {
		const auto lastVal = (maxHint < lastVal_) ? (maxHint + 1) : lastVal_;

		int maxVal = INT_MIN;
		const auto beg = begin();
		for (auto it = beg, endIt = end(); it != endIt; ++it) {
			if (it->useBtree_ && it->ritset_ != it->setrend_) {
				for (; it->ritset_ != it->setrend_ && *it->ritset_ >= lastVal; ++it->ritset_) {
				}
				if (it->ritset_ != it->setrend_ && *it->ritset_ > maxVal) {
					maxVal = *it->ritset_;
					lastPos_ = it - beg;
				}
			} else if (it->isRange_ && it->rrIt_ != it->rrEnd_) {
				it->rrIt_ = std::max(it->rrEnd_, std::min(it->rrIt_, lastVal - 1));

				if (it->rrIt_ != it->rrEnd_ && it->rrIt_ > maxVal) {
					maxVal = it->rrIt_;
					lastPos_ = it - beg;
				}
			} else if (!it->isRange_ && !it->useBtree_ && it->rit_ != it->rend_) {
				for (; it->rit_ != it->rend_ && *it->rit_ >= lastVal; ++it->rit_) {
				}
				if (it->rit_ != it->rend_ && *it->rit_ > maxVal) {
					maxVal = *it->rit_;
					lastPos_ = it - beg;
				}
			}
		}
		lastVal_ = maxVal;
		return !(lastVal_ == INT_MIN);
	}
	// Single range next implementation
	bool nextFwdSingleRange(IdType minHint) noexcept {
		if (minHint > lastVal_) {
			lastVal_ = minHint - 1;
		}

		const auto begIt = begin();
		if (lastVal_ < begIt->rBegin_) {
			lastVal_ = begIt->rBegin_ - 1;
		}

		lastVal_ = (lastVal_ < begIt->rEnd_) ? lastVal_ + 1 : begIt->rEnd_;
		if (lastVal_ == begIt->rEnd_) {
			lastVal_ = INT_MAX;
		}
		return (lastVal_ != INT_MAX);
	}
	// Single idset next implementation
	bool nextFwdSingleIdset(IdType minHint) noexcept {
		const auto lastVal = (minHint > lastVal_) ? (minHint - 1) : lastVal_;

		auto it = begin();
		if (it->useBtree_) {
			if (it->itset_ != it->setend_ && *it->itset_ <= lastVal) {
				it->itset_ = it->set_->upper_bound(lastVal);
			}
			lastVal_ = (it->itset_ != it->set_->end()) ? *it->itset_ : INT_MAX;
		} else {
			if (it->bsearch_) {
				if (it->it_ != it->end_ && *it->it_ <= lastVal) {
					it->it_ = std::upper_bound(it->it_, it->end_, lastVal);
				}
			} else {
				for (; it->it_ != it->end_ && *it->it_ <= lastVal; it->it_++) {
				}
			}
			lastVal_ = (it->it_ != it->end_) ? *it->it_ : INT_MAX;
		}
		return !(lastVal_ == INT_MAX);
	}
	bool nextRevSingleRange(IdType maxHint) noexcept {
		if (maxHint < lastVal_) {
			lastVal_ = maxHint + 1;
		}

		const auto begIt = begin();
		if (lastVal_ > begIt->rrBegin_) {
			lastVal_ = begIt->rrBegin_ + 1;
		}

		lastVal_ = (lastVal_ > begIt->rrEnd_) ? lastVal_ - 1 : begIt->rrEnd_;
		if (lastVal_ == begIt->rrEnd_) {
			lastVal_ = INT_MIN;
		}
		return (lastVal_ != INT_MIN);
	}
	bool nextRevSingleIdset(IdType maxHint) noexcept {
		const auto lastVal = (maxHint < lastVal_) ? (maxHint + 1) : lastVal_;

		auto it = begin();

		if (it->useBtree_) {
			for (; it->ritset_ != it->setrend_ && *it->ritset_ >= lastVal; ++it->ritset_) {
			}
			lastVal_ = (it->ritset_ != it->setrend_) ? *it->ritset_ : INT_MIN;
		} else {
			for (; it->rit_ != it->rend_ && *it->rit_ >= lastVal; ++it->rit_) {
			}
			lastVal_ = (it->rit_ != it->rend_) ? *it->rit_ : INT_MIN;
		}

		return !(lastVal_ == INT_MIN);
	}
	bool nextUnbuiltSortOrders() noexcept {
		auto& iter = *begin()->indexForwardIter_;
		const bool res = iter.Next();
		lastVal_ = iter.Value();
		return res;
	}
	// Unsorted next implementation
	bool nextUnsorted() noexcept {
		if (lastPos_ == size()) {
			return false;
		}
		if (operator[](lastPos_).it_ == operator[](lastPos_).end_) {
			++lastPos_;
			while (lastPos_ < size()) {
				if (operator[](lastPos_).it_ != operator[](lastPos_).end_) {
					lastVal_ = *(operator[](lastPos_).it_++);
					return true;
				}
				++lastPos_;
			}
			return false;
		}
		lastVal_ = *(operator[](lastPos_).it_++);
		return true;
	}

	/// Performs ID sets merge and sort in case, when this sort was deferred earlier and still effective with current maxIterations value
	bool applyDeferedSort(int maxIterations) {
		if (deferedExplicitSort && maxIterations > 0 && !distinct_) {
			const auto idsCount = GetMaxIterations();
			if (IsGenericSortRecommended(size(), idsCount, size_t(maxIterations))) {
				[[maybe_unused]] auto merged =
					MergeIdsets(SelectKeyResult::MergeOptions{.genericSort = true, .shrinkResult = false}, idsCount);
				return true;
			}
		}
		return false;
	}

	int totalCalls_ = 0;
	IdType lastVal_ = INT_MIN;
	int type_ = 0;
	bool isUnsorted_ = false;
	bool isReverse_ = false;
	ForcedFirst forcedFirst_ = ForcedFirst_False;
	bool isNotOperation_ = false;
	size_t lastPos_ = 0;
	int matchedCount_ = 0;
	int indexNo_ = IndexValueType::NotSet;
	reindexer::IsDistinct distinct_ = IsDistinct_False;
};

}  // namespace reindexer
