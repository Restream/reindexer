
#include "selectiterator.h"

#include <algorithm>
#include <cmath>
#include "core/index/indexiterator.h"

namespace reindexer {

void SelectIterator::Bind(const PayloadType &type, int field) {
	for (Comparator &cmp : comparators_) cmp.Bind(type, field);
}

void SelectIterator::Start(bool reverse, int maxIterations) {
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

	if (size() == 0) {
		type_ = OnlyComparator;
		lastVal_ = isReverse_ ? INT_MIN : INT_MAX;
	} else if (size() == 1 && begIt->indexForwardIter_) {
		type_ = UnbuiltSortOrdersIndex;
		begIt->indexForwardIter_->Start(reverse);
	} else if (isUnsorted) {
		type_ = Unsorted;
	} else if (size() == 1) {
		if (!isReverse_) {
			type_ = begIt->isRange_ ? SingleRange : (explicitSort ? SingleIdSetWithDeferedSort : SingleIdset);
		} else {
			type_ = begIt->isRange_ ? RevSingleRange : (explicitSort ? RevSingleIdSetWithDeferedSort : RevSingleIdset);
		}
	} else {
		type_ = isReverse_ ? Reverse : Forward;
	}

	ClearDistinct();
}

// Generic next implementation
bool SelectIterator::nextFwd(IdType minHint) noexcept {
	if (minHint > lastVal_) lastVal_ = minHint - 1;
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

bool SelectIterator::nextRev(IdType maxHint) noexcept {
	if (maxHint < lastVal_) lastVal_ = maxHint + 1;

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

// Single idset next implementation
bool SelectIterator::nextFwdSingleIdset(IdType minHint) noexcept {
	if (minHint > lastVal_) lastVal_ = minHint - 1;
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
			for (; it->it_ != it->end_ && *it->it_ <= lastVal_; ++it->it_) {
			}
		}
		lastVal_ = (it->it_ != it->end_) ? *it->it_ : INT_MAX;
	}
	return !(lastVal_ == INT_MAX);
}

bool SelectIterator::nextRevSingleIdset(IdType maxHint) noexcept {
	if (maxHint < lastVal_) lastVal_ = maxHint + 1;

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

bool SelectIterator::nextUnbuiltSortOrders() noexcept { return begin()->indexForwardIter_->Next(); }

// Single range next implementation
bool SelectIterator::nextFwdSingleRange(IdType minHint) noexcept {
	if (minHint > lastVal_) lastVal_ = minHint - 1;

	const auto begIt = begin();
	if (lastVal_ < begIt->rBegin_) lastVal_ = begIt->rBegin_ - 1;

	lastVal_ = (lastVal_ < begIt->rEnd_) ? lastVal_ + 1 : begIt->rEnd_;
	if (lastVal_ == begIt->rEnd_) lastVal_ = INT_MAX;
	return (lastVal_ != INT_MAX);
}

bool SelectIterator::nextRevSingleRange(IdType maxHint) noexcept {
	if (maxHint < lastVal_) lastVal_ = maxHint + 1;

	const auto begIt = begin();
	if (lastVal_ > begIt->rrBegin_) lastVal_ = begIt->rrBegin_ + 1;

	lastVal_ = (lastVal_ > begIt->rrEnd_) ? lastVal_ - 1 : begIt->rrEnd_;
	if (lastVal_ == begIt->rrEnd_) lastVal_ = INT_MIN;
	return (lastVal_ != INT_MIN);
}

// Unsorted next implementation
bool SelectIterator::nextUnsorted() noexcept {
	const auto endIt = end();
	if (lastIt_ == endIt) {
		return false;
	} else if (lastIt_->it_ == lastIt_->end_) {
		++lastIt_;

		while (lastIt_ != endIt) {
			if (lastIt_->it_ != lastIt_->end_) {
				lastVal_ = *lastIt_->it_;
				++lastIt_->it_;
				return true;
			}
			++lastIt_;
		}
	} else {
		lastVal_ = *lastIt_->it_;
		++lastIt_->it_;
		return true;
	}

	return false;
}

void SelectIterator::ExcludeLastSet(const PayloadValue &value, IdType rowId, IdType properRowId) {
	for (auto &comp : comparators_) comp.ExcludeDistinct(value, properRowId);
	if (type_ == UnbuiltSortOrdersIndex) {
		const auto begIt = begin();
		if (begIt->indexForwardIter_->Value() == rowId) {
			begIt->indexForwardIter_->ExcludeLastSet();
		}
	} else if (!End() && lastIt_ != end() && lastVal_ == rowId) {
		assertrx(!lastIt_->isRange_);
		if (lastIt_->useBtree_) {
			lastIt_->itset_ = lastIt_->setend_;
			lastIt_->ritset_ = lastIt_->setrend_;
		} else {
			lastIt_->it_ = lastIt_->end_;
			lastIt_->rit_ = lastIt_->rend_;
		}
	}
}

void SelectIterator::Append(SelectKeyResult &other) {
	reserve(size() + other.size());
	for (auto &r : other) emplace_back(std::move(r));
	comparators_.reserve(comparators_.size() + other.comparators_.size());
	for (auto &c : other.comparators_) {
		comparators_.emplace_back(std::move(c));
	}
}

void SelectIterator::AppendAndBind(SelectKeyResult &other, const PayloadType &type, int field) {
	reserve(size() + other.size());
	for (auto &r : other) emplace_back(std::move(r));
	comparators_.reserve(comparators_.size() + other.comparators_.size());
	for (auto &c : other.comparators_) {
		c.Bind(type, field);
		comparators_.emplace_back(std::move(c));
	}
}

double SelectIterator::Cost(int expectedIterations) const noexcept {
	if (type_ == UnbuiltSortOrdersIndex) {
		return -1;
	}
	if (forcedFirst_) {
		return -GetMaxIterations();
	}
	double result{0.0};
	if (!comparators_.empty()) {
		const auto jsonPathComparators =
			std::count_if(comparators_.begin(), comparators_.end(), [](const Comparator &c) noexcept { return c.HasJsonPaths(); });
		// Comparatos with non index fields must have much higher cost, than comparators with index fields
		result = jsonPathComparators ? (kNonIdxFieldComparatorCostMultiplier * double(expectedIterations) + jsonPathComparators + 1)
									 : (double(expectedIterations) + 1);
	}
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

IdType SelectIterator::Val() const noexcept {
	if (type_ == UnbuiltSortOrdersIndex) {
		return begin()->indexForwardIter_->Value();
	} else {
		return lastVal_;
	}
}

void SelectIterator::SetExpectMaxIterations(int expectedIterations) noexcept {
	for (SingleSelectKeyResult &r : *this) {
		if (!r.isRange_ && r.ids_.size() > 1) {
			int itersloop = r.ids_.size();
			int itersbsearch = int((std::log2(r.ids_.size()) - 1) * expectedIterations);
			r.bsearch_ = itersbsearch < itersloop;
		}
	}
}

std::string_view SelectIterator::TypeName() const noexcept {
	using namespace std::string_view_literals;
	switch (type_) {
		case Forward:
			return "Forward"sv;
		case Reverse:
			return "Reverse"sv;
		case SingleRange:
			return "SingleRange"sv;
		case SingleIdset:
			return "SingleIdset"sv;
		case SingleIdSetWithDeferedSort:
			return "SingleIdSetWithDeferedSort"sv;
		case RevSingleRange:
			return "RevSingleRange"sv;
		case RevSingleIdset:
			return "RevSingleIdset"sv;
		case RevSingleIdSetWithDeferedSort:
			return "RevSingleIdSetWithDeferedSort"sv;
		case OnlyComparator:
			return "OnlyComparator"sv;
		case Unsorted:
			return "Unsorted"sv;
		case UnbuiltSortOrdersIndex:
			return "UnbuiltSortOrdersIndex"sv;
		default:
			return "<unknown>"sv;
	}
}

std::string SelectIterator::Dump() const {
	std::string ret = name + ' ' + std::string(TypeName()) + "(";

	for (auto &it : *this) {
		if (it.useBtree_) ret += "btree;";
		if (it.isRange_) ret += "range;";
		if (it.bsearch_) ret += "bsearch;";
		ret += ",";
		if (ret.length() > 256) {
			ret += "...";
			break;
		}
	}
	ret += ")";
	return ret;
}

}  // namespace reindexer
