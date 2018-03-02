
#include "selectiterator.h"
#include <algorithm>
#include <cmath>

namespace reindexer {

using std::min;
using std::max;

void SelectIterator::Bind(PayloadType type, int field) {
	for (auto &cmp : comparators_) cmp.Bind(type, field);
}

void SelectIterator::Start(bool reverse) {
	reverse_ = reverse;
	lastIt_ = begin();

	for (auto it = begin(); it != end(); it++) {
		if (it->isRange_) {
			if (reverse_) {
				auto rrBegin = it->rEnd_ - 1;
				it->rrEnd_ = it->rBegin_ - 1;
				it->rrBegin_ = rrBegin;
				it->rrIt_ = it->rrBegin_;
			} else {
				it->rIt_ = it->rBegin_;
			}
		} else {
			if (!reverse_) {
				it->begin_ = it->ids_.begin();
				it->end_ = it->ids_.end();
				it->it_ = it->ids_.begin();
			} else {
				it->rbegin_ = it->ids_.rbegin();
				it->rend_ = it->ids_.rend();
				it->rit_ = it->ids_.rbegin();
			}
		}
	}

	lastVal_ = reverse_ ? INT_MAX : INT_MIN;
	type_ = reverse_ ? Reverse : Forward;
	if (is_unsorted) {
		type_ = Unsorted;

	} else if (size() == 1 && !reverse_) {
		type_ = begin()->isRange_ ? SingleRange : SingleIdset;
	} else if (size() == 1) {
		type_ = begin()->isRange_ ? RevSingleRange : RevSingleIdset;
	}
	if (size() == 0) {
		type_ = OnlyComparator;
		lastVal_ = reverse_ ? INT_MIN : INT_MAX;
	}
}

// Generic next implementation
// ***************************
bool SelectIterator::nextFwd(IdType minHint) {
	if (minHint > lastVal_) lastVal_ = minHint - 1;

	int minVal = INT_MAX;
	for (auto it = begin(); it != end(); it++) {
		if (it->isRange_ && it->rIt_ != it->rEnd_) {
			it->rIt_ = min(it->rEnd_, max(it->rIt_, lastVal_ + 1));

			if (it->rIt_ != it->rEnd_ && it->rIt_ < minVal) {
				minVal = it->rIt_;
				lastIt_ = it;
			}

		} else if (!it->isRange_ && it->it_ != it->end_) {
			for (; it->it_ != it->end_ && *it->it_ <= lastVal_; it->it_++) {
			}
			if (it->it_ != it->end_ && *it->it_ < minVal) {
				minVal = *it->it_;
				lastIt_ = it;
			}
		}
	}
	lastVal_ = minVal;
	return lastVal_ != INT_MAX;
}

bool SelectIterator::nextRev(IdType maxHint) {
	if (maxHint < lastVal_) lastVal_ = maxHint + 1;

	int maxVal = INT_MIN;
	for (auto it = begin(); it != end(); it++) {
		if (it->isRange_ && it->rrIt_ != it->rrEnd_) {
			it->rrIt_ = max(it->rrEnd_, min(it->rrIt_, lastVal_ - 1));

			if (it->rrIt_ != it->rrEnd_ && it->rrIt_ > maxVal) {
				maxVal = it->rrIt_;
				lastIt_ = it;
			}
		} else if (!it->isRange_ && it->rit_ != it->rend_) {
			for (; it->rit_ != it->rend_ && *it->rit_ >= lastVal_; it->rit_++) {
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
// ********************************
bool SelectIterator::nextFwdSingleIdset(IdType minHint) {
	if (minHint > lastVal_) lastVal_ = minHint - 1;

	auto it = begin();
	if (it->bsearch_) {
		if (it->it_ != it->end_ && *it->it_ <= lastVal_) it->it_ = std::lower_bound(it->it_ + 1, it->end_, lastVal_);
	} else
		for (; it->it_ != it->end_ && *it->it_ <= lastVal_; it->it_++) {
		}
	lastVal_ = (it->it_ != it->end_) ? *it->it_ : INT_MAX;

	return !(lastVal_ == INT_MAX);
}

bool SelectIterator::nextRevSingleIdset(IdType maxHint) {
	if (maxHint < lastVal_) lastVal_ = maxHint + 1;

	auto it = begin();
	for (; it->rit_ != it->rend_ && *it->rit_ >= lastVal_; it->rit_++) {
	}

	lastVal_ = (it->rit_ != it->rend_) ? *it->rit_ : INT_MIN;
	return !(lastVal_ == INT_MIN);
}

// Single range next implementation
// ********************************
bool SelectIterator::nextFwdSingleRange(IdType minHint) {
	if (minHint > lastVal_) lastVal_ = minHint - 1;

	if (lastVal_ < begin()->rBegin_) lastVal_ = begin()->rBegin_ - 1;

	lastVal_ = (lastVal_ < begin()->rEnd_) ? lastVal_ + 1 : begin()->rEnd_;
	if (lastVal_ == begin()->rEnd_) lastVal_ = INT_MAX;
	return (lastVal_ != INT_MAX);
}

bool SelectIterator::nextRevSingleRange(IdType maxHint) {
	if (maxHint < lastVal_) lastVal_ = maxHint + 1;

	if (lastVal_ > begin()->rrBegin_) lastVal_ = begin()->rrBegin_ + 1;

	lastVal_ = (lastVal_ > begin()->rrEnd_) ? lastVal_ - 1 : begin()->rrEnd_;
	if (lastVal_ == begin()->rrEnd_) lastVal_ = INT_MIN;
	return (lastVal_ != INT_MIN);
}
// Unsorted next implementation
// ********************************
bool SelectIterator::nextUnsorted() {
	if (lastIt_ == end()) {
		return false;
	} else if (lastIt_->it_ == lastIt_->end_) {
		++lastIt_;

		while (lastIt_ != end()) {
			if (lastIt_->it_ != lastIt_->end_) {
				lastVal_ = *lastIt_->it_;
				lastIt_->it_++;
				return true;
			}
			++lastIt_;
		}
	} else {
		lastVal_ = *lastIt_->it_;
		lastIt_->it_++;
		return true;
	}

	return false;
}

void SelectIterator::ExcludeLastSet() {
	if (!End() && lastIt_ != end()) {
		assert(!lastIt_->isRange_);
		lastIt_->it_ = lastIt_->end_;
		lastIt_->rit_ = lastIt_->rend_;
	}
	assert(!comparators_.size());
}

void SelectIterator::AppendAndBind(SelectKeyResult &other, PayloadType type, int field) {
	for (auto &r : other) push_back(std::move(r));
	for (auto &c : other.comparators_) {
		c.Bind(type, field);
		comparators_.push_back(std::move(c));
	}
}

double SelectIterator::Cost(int expectedIterations) const {
	if (forcedFirst_) return -GetMaxIterations();

	if (size() < 2 && !comparators_.size()) return double(GetMaxIterations());

	if (comparators_.size()) return expectedIterations + GetMaxIterations() * size() + 1;

	return GetMaxIterations() * size();
}

void SelectIterator::SetExpectMaxIterations(int expectedIterations) {
	for (auto &r : *this) {
		if (!r.isRange_ && r.ids_.size() > 1) {
			int itersloop = r.ids_.size();
			int itersbsearch = int((std::log2(r.ids_.size()) - 1) * expectedIterations);
			r.bsearch_ = itersbsearch < itersloop;
		}
	}
}

int SelectIterator::GetMaxIterations() const {
	int cnt = 0;
	for (auto &r : *this) cnt += r.isRange_ ? (r.rEnd_ - r.rBegin_) : r.ids_.size();
	return cnt;
}

}  // namespace reindexer
