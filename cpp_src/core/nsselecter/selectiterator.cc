
#include "selectiterator.h"
#include <algorithm>
#include <cmath>

namespace reindexer {

using std::min;
using std::max;

SelectIterator::SelectIterator() {}
SelectIterator::SelectIterator(const SelectKeyResult &res, OpType _op, bool _distinct, const string &_name, bool forcedFirst)
	: SelectKeyResult(res), op(_op), distinct(_distinct), name(_name), forcedFirst_(forcedFirst), type_(Forward) {}

void SelectIterator::Bind(PayloadType type, int field) {
	for (Comparator &cmp : comparators_) cmp.Bind(type, field);
}

void SelectIterator::Start(bool reverse) {
	isReverse_ = reverse;
	lastIt_ = begin();

	for (auto it = begin(); it != end(); it++) {
		if (it->isRange_) {
			if (isReverse_) {
				auto rrBegin = it->rEnd_ - 1;
				it->rrEnd_ = it->rBegin_ - 1;
				it->rrBegin_ = rrBegin;
				it->rrIt_ = it->rrBegin_;
			} else {
				it->rIt_ = it->rBegin_;
			}
		} else {
			if (it->useBtree_) {
				assert(it->set_);
				if (reverse) {
					it->setrbegin_ = it->set_->rbegin();
					it->setrend_ = it->set_->rend();
					it->ritset_ = it->set_->rbegin();
				} else {
					it->setbegin_ = it->set_->begin();
					it->setend_ = it->set_->end();
					it->itset_ = it->setbegin_;
				}
			} else {
				if (isReverse_) {
					it->rbegin_ = it->ids_.rbegin();
					it->rend_ = it->ids_.rend();
					it->rit_ = it->ids_.rbegin();
				} else {
					it->begin_ = it->ids_.begin();
					it->end_ = it->ids_.end();
					it->it_ = it->ids_.begin();
				}
			}
		}
	}

	lastVal_ = isReverse_ ? INT_MAX : INT_MIN;
	type_ = isReverse_ ? Reverse : Forward;
	if (isUnsorted) {
		type_ = Unsorted;

	} else if (size() == 1 && !isReverse_) {
		type_ = begin()->isRange_ ? SingleRange : SingleIdset;
	} else if (size() == 1) {
		type_ = begin()->isRange_ ? RevSingleRange : RevSingleIdset;
	}
	if (size() == 0) {
		type_ = OnlyComparator;
		lastVal_ = isReverse_ ? INT_MIN : INT_MAX;
	}
}

// Generic next implementation
bool SelectIterator::nextFwd(IdType minHint) {
	if (minHint > lastVal_) lastVal_ = minHint - 1;
	int minVal = INT_MAX;
	for (auto it = begin(); it != end(); it++) {
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
	}
	lastVal_ = minVal;
	return lastVal_ != INT_MAX;
}

bool SelectIterator::nextRev(IdType maxHint) {
	if (maxHint < lastVal_) lastVal_ = maxHint + 1;

	int maxVal = INT_MIN;
	for (auto it = begin(); it != end(); it++) {
		if (it->useBtree_ && it->ritset_ != it->setrend_) {
			for (; it->ritset_ != it->setrend_ && *it->ritset_ >= lastVal_; ++it->ritset_) {
			}
			if (it->ritset_ != it->setrend_ && *it->ritset_ > maxVal) {
				maxVal = *it->ritset_;
				lastIt_ = it;
			}
		}
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
bool SelectIterator::nextFwdSingleIdset(IdType minHint) {
	if (minHint > lastVal_) lastVal_ = minHint - 1;
	auto it = begin();
	if (it->useBtree_) {
		if (it->itset_ != it->setend_ && *it->it_ >= lastVal_) {
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

bool SelectIterator::nextRevSingleIdset(IdType maxHint) {
	if (maxHint < lastVal_) lastVal_ = maxHint + 1;

	auto it = begin();

	if (it->useBtree_) {
		for (; it->ritset_ != it->setrend_ && *it->ritset_ >= lastVal_; it->ritset_++) {
		}
		lastVal_ = (it->ritset_ != it->setrend_) ? *it->ritset_ : INT_MIN;
	} else {
		for (; it->rit_ != it->rend_ && *it->rit_ >= lastVal_; it->rit_++) {
		}
		lastVal_ = (it->rit_ != it->rend_) ? *it->rit_ : INT_MIN;
	}

	return !(lastVal_ == INT_MIN);
}

// Single range next implementation
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
	for (auto &r : other) push_back(std::move(r));
	for (auto &c : other.comparators_) {
		comparators_.push_back(std::move(c));
	}
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
	for (SingleSelectKeyResult &r : *this) {
		if (!r.isRange_ && r.ids_.size() > 1) {
			int itersloop = r.ids_.size();
			int itersbsearch = int((std::log2(r.ids_.size()) - 1) * expectedIterations);
			r.bsearch_ = itersbsearch < itersloop;
		}
	}
}

int SelectIterator::GetMaxIterations() const {
	int cnt = 0;
	for (const SingleSelectKeyResult &r : *this) {
		if (r.isRange_) {
			cnt += std::abs(r.rEnd_ - r.rBegin_);
		} else if (r.useBtree_) {
			cnt += r.set_->size();
		} else {
			cnt += r.ids_.size();
		}
	}
	return cnt;
}

const char *SelectIterator::TypeName() const {
	switch (type_) {
		case Forward:
			return "Forward";
		case Reverse:
			return "Reverse";
		case SingleRange:
			return "SingleRange";
		case SingleIdset:
			return "SingleIdset";
		case RevSingleRange:
			return "RevSingleRange";
		case RevSingleIdset:
			return "RevSingleIdset";
		case OnlyComparator:
			return "OnlyComparator";
		case Unsorted:
			return "Unsorted";
		default:
			return "<unknown>";
	}
}

string SelectIterator::Dump() const {
	string ret = string(TypeName()) + "(";

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
