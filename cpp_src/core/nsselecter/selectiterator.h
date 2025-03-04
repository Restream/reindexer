#pragma once

#include "core/enums.h"
#include "core/selectkeyresult.h"

namespace reindexer {

enum class IteratorFieldKind { None, NonIndexed, Indexed };
/// Allows to iterate over a result of selecting
/// data for one certain key.
class SelectIterator : public SelectKeyResult {
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
		OnlyComparator,	 // TODO delete this #1585
		Unsorted,
		UnbuiltSortOrdersIndex,
	};

	template <typename Str, std::enable_if_t<std::is_constructible_v<std::string, Str>>* = nullptr>
	SelectIterator(SelectKeyResult&& res, bool dist, Str&& n, IteratorFieldKind fKind, ForcedFirst forcedFirst = ForcedFirst_False) noexcept
		: SelectKeyResult(std::move(res)),
		  distinct(dist),
		  name(std::forward<Str>(n)),
		  fieldKind(fKind),
		  forcedFirst_(forcedFirst),
		  type_(Forward) {}

	/// Starts iteration process: prepares
	/// object for further work.
	/// @param reverse - direction of iteration.
	/// @param maxIterations - expected max iterations in select loop
	void Start(bool reverse, int maxIterations) {
		const bool explicitSort = applyDeferedSort(maxIterations);

		isReverse_ = reverse;
		const auto begIt = begin();
		lastIt_ = begIt;

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
		const auto sz = size();
		if (sz == 0) {
			type_ = OnlyComparator;
			lastVal_ = isReverse_ ? INT_MIN : INT_MAX;
		} else if (sz == 1 && begIt->indexForwardIter_) {
			type_ = UnbuiltSortOrdersIndex;
			begIt->indexForwardIter_->Start(reverse);
		} else if (isUnsorted) {
			type_ = Unsorted;
		} else if (sz == 1) {
			if (!isReverse_) {
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
	/// Iterates to a next item of result.
	/// @param minHint - rowId value to start from.
	/// @return true if operation succeed.
	RX_ALWAYS_INLINE bool Next(IdType minHint) {
		bool res = false;
		switch (type_) {
			case Forward:
				res = nextFwd(minHint);
				break;
			case Reverse:
				res = nextRev(minHint);
				break;
			case SingleRange:
				res = nextFwdSingleRange(minHint);
				break;
			case SingleIdset:
			case SingleIdSetWithDeferedSort:
				res = nextFwdSingleIdset(minHint);
				break;
			case RevSingleRange:
				res = nextRevSingleRange(minHint);
				break;
			case RevSingleIdset:
			case RevSingleIdSetWithDeferedSort:
				res = nextRevSingleIdset(minHint);
				break;
			case OnlyComparator:
				return false;
			case Unsorted:
				res = nextUnsorted();
				break;
			case UnbuiltSortOrdersIndex:
				res = nextUnbuiltSortOrders();
				break;
		}
		if (res) {
			++matchedCount_;
		}
		return res;
	}

	/// Sets Unsorted iteration mode
	RX_ALWAYS_INLINE void SetUnsorted() noexcept { isUnsorted = true; }

	/// Current rowId
	RX_ALWAYS_INLINE IdType Val() const noexcept {
		return (type_ == UnbuiltSortOrdersIndex) ? begin()->indexForwardIter_->Value() : lastVal_;
	}

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
			case OnlyComparator:
			case Unsorted:
			case UnbuiltSortOrdersIndex:
			default:
				assertrx_throw(!lastIt_->useBtree_ && (type_ != UnbuiltSortOrdersIndex));
				return lastIt_->it_ - lastIt_->begin_ - 1;
		}
	}

	/// @return amonut of matched items
	int GetMatchedCount() const noexcept { return matchedCount_; }

	/// Excludes last set of ids from each result
	/// to remove duplicated keys
	void ExcludeLastSet(IdType rowId) {
		if (type_ == UnbuiltSortOrdersIndex) {
			auto fwdIter = begin()->indexForwardIter_;
			if (fwdIter->Value() == rowId) {
				fwdIter->ExcludeLastSet();
			}
		} else if (!End() && lastIt_ != end() && lastVal_ == rowId) {
			assertrx_throw(!lastIt_->isRange_);
			if (lastIt_->useBtree_) {
				lastIt_->itset_ = lastIt_->setend_;
				lastIt_->ritset_ = lastIt_->setrend_;
			} else {
				lastIt_->it_ = lastIt_->end_;
				lastIt_->rit_ = lastIt_->rend_;
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
		if (distinct) {
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
		for (SingleSelectKeyResult& r : *this) {
			if (!r.isRange_ && r.ids_.size() > 1) {
				int itersloop = r.ids_.size();
				int itersbsearch = int((std::log2(r.ids_.size()) - 1) * expectedIterations);
				r.bsearch_ = itersbsearch < itersloop;
			}
		}
	}

	int Type() const noexcept { return type_; }

	std::string_view TypeName() const noexcept;
	std::string Dump() const;

	bool distinct = false;
	std::string name;
	IteratorFieldKind fieldKind = IteratorFieldKind::None;

protected:
	// Iterates to a next item of result
	// depending on iterator type starting
	// from minHint which is the least rowId.
	// Generic next implementation
	bool nextFwd(IdType minHint) noexcept {
		if (minHint > lastVal_) {
			lastVal_ = minHint - 1;
		}
		int minVal = INT_MAX;
		for (auto it = begin(), endIt = end(); it != endIt; ++it) {
			if (it->useBtree_) {
				if (it->itset_ != it->setend_) {
					it->itset_ = it->set_->upper_bound(lastVal_);
					if (it->itset_ != it->setend_ && *it->itset_ < minVal) {
						minVal = *it->itset_;
						lastIt_ = it;
					}
				}
			} else {
				if (it->isRange_ && it->rIt_ != it->rEnd_) {
					it->rIt_ = std::min(it->rEnd_, std::max(it->rIt_, lastVal_ + 1));

					if (it->rIt_ != it->rEnd_ && it->rIt_ < minVal) {
						minVal = it->rIt_;
						lastIt_ = it;
					}

				} else if (!it->isRange_ && it->it_ != it->end_) {
					for (; it->it_ != it->end_ && *it->it_ <= lastVal_; ++it->it_) {
					}
					if (it->it_ != it->end_ && *it->it_ < minVal) {
						minVal = *it->it_;
						lastIt_ = it;
					}
				}
			}
		}
		lastVal_ = minVal;
		return lastVal_ != INT_MAX;
	}
	bool nextRev(IdType maxHint) noexcept {
		if (maxHint < lastVal_) {
			lastVal_ = maxHint + 1;
		}

		int maxVal = INT_MIN;
		for (auto it = begin(), endIt = end(); it != endIt; ++it) {
			if (it->useBtree_ && it->ritset_ != it->setrend_) {
				for (; it->ritset_ != it->setrend_ && *it->ritset_ >= lastVal_; ++it->ritset_) {
				}
				if (it->ritset_ != it->setrend_ && *it->ritset_ > maxVal) {
					maxVal = *it->ritset_;
					lastIt_ = it;
				}
			} else if (it->isRange_ && it->rrIt_ != it->rrEnd_) {
				it->rrIt_ = std::max(it->rrEnd_, std::min(it->rrIt_, lastVal_ - 1));

				if (it->rrIt_ != it->rrEnd_ && it->rrIt_ > maxVal) {
					maxVal = it->rrIt_;
					lastIt_ = it;
				}
			} else if (!it->isRange_ && !it->useBtree_ && it->rit_ != it->rend_) {
				for (; it->rit_ != it->rend_ && *it->rit_ >= lastVal_; ++it->rit_) {
				}
				if (it->rit_ != it->rend_ && *it->rit_ > maxVal) {
					maxVal = *it->rit_;
					lastIt_ = it;
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
		if (minHint > lastVal_) {
			lastVal_ = minHint - 1;
		}
		auto it = begin();
		if (it->useBtree_) {
			if (it->itset_ != it->setend_ && *it->itset_ <= lastVal_) {
				it->itset_ = it->set_->upper_bound(lastVal_);
			}
			lastVal_ = (it->itset_ != it->set_->end()) ? *it->itset_ : INT_MAX;
		} else {
			if (it->bsearch_) {
				if (it->it_ != it->end_ && *it->it_ <= lastVal_) {
					it->it_ = std::upper_bound(it->it_, it->end_, lastVal_);
				}
			} else {
				for (; it->it_ != it->end_ && *it->it_ <= lastVal_; it->it_++) {
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
		if (maxHint < lastVal_) {
			lastVal_ = maxHint + 1;
		}

		auto it = begin();

		if (it->useBtree_) {
			for (; it->ritset_ != it->setrend_ && *it->ritset_ >= lastVal_; ++it->ritset_) {
			}
			lastVal_ = (it->ritset_ != it->setrend_) ? *it->ritset_ : INT_MIN;
		} else {
			for (; it->rit_ != it->rend_ && *it->rit_ >= lastVal_; ++it->rit_) {
			}
			lastVal_ = (it->rit_ != it->rend_) ? *it->rit_ : INT_MIN;
		}

		return !(lastVal_ == INT_MIN);
	}
	bool nextUnbuiltSortOrders() noexcept { return begin()->indexForwardIter_->Next(); }
	// Unsorted next implementation
	bool nextUnsorted() noexcept {
		const auto endIt = end();
		if (lastIt_ == endIt) {
			return false;
		}
		if (lastIt_->it_ == lastIt_->end_) {
			++lastIt_;
			while (lastIt_ != endIt) {
				if (lastIt_->it_ != lastIt_->end_) {
					lastVal_ = *(lastIt_->it_++);
					return true;
				}
				++lastIt_;
			}
			return false;
		}
		lastVal_ = *(lastIt_->it_++);
		return true;
	}

	/// Performs ID sets merge and sort in case, when this sort was defered earlier and still effective with current maxIterations value
	bool applyDeferedSort(int maxIterations) {
		if (deferedExplicitSort && maxIterations > 0 && !distinct) {
			const auto idsCount = GetMaxIterations();
			if (IsGenericSortRecommended(size(), idsCount, size_t(maxIterations))) {
				MergeIdsets(SelectKeyResult::MergeOptions{.genericSort = true, .shrinkResult = false}, idsCount);
				return true;
			}
		}
		return false;
	}

	bool isUnsorted = false;
	bool isReverse_ = false;
	ForcedFirst forcedFirst_ = ForcedFirst_False;
	bool isNotOperation_ = false;
	int type_ = 0;
	iterator lastIt_ = nullptr;
	IdType lastVal_ = INT_MIN;
	IdType end_ = 0;
	int matchedCount_ = 0;
};

}  // namespace reindexer
