#pragma once

#include "core/selectkeyresult.h"

namespace reindexer {

class SelectIterator : public SelectKeyResult {
public:
	enum {
		Forward,
		Reverse,
		SingleRange,
		SingleIdset,
		RevSingleRange,
		RevSingleIdset,
		OnlyComparator,
		Unsorted,
	};

	SelectIterator() {}
	SelectIterator(const SelectKeyResult &res, OpType _op, bool _distinct, const string &_name, bool forcedFirst = false)
		: SelectKeyResult(res), op(_op), distinct(_distinct), name(_name), forcedFirst_(forcedFirst), type_(Forward) {}

	// Iteration
	void SetUnsorted() { is_unsorted = true; }
	void Start(bool reverse);
	bool End() { return lastVal_ == (reverse_ ? INT_MIN : INT_MAX) && !comparators_.size(); }
	bool Next(IdType minHint) {
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
				res = nextFwdSingleIdset(minHint);
				break;
			case RevSingleRange:
				res = nextRevSingleRange(minHint);
				break;
			case RevSingleIdset:
				res = nextRevSingleIdset(minHint);
				break;
			case OnlyComparator:
				return false;
			case Unsorted:
				res = nextUnsorted();
				break;
		}
		if (res) matchedCount_++;
		return res;
	}
	// Iteration values
	int Val() { return lastVal_; }
	int Pos() { return lastIt_->it_ - lastIt_->begin_ - 1; }

	// Comparators stuff
	void Bind(const PayloadType *type, int field);
	bool TryCompare(const PayloadValue *item, int idx) {
		for (auto &cmp : comparators_)
			if (cmp.Compare(*item, idx)) {
				matchedCount_++;
				return true;
			}
		return false;
	}
	int GetMatchedCount() { return matchedCount_; }
	void ExcludeLastSet();
	void AppendAndBind(SelectKeyResult &other, const PayloadType *type, int field);
	double Cost(int totalIds) const;
	int GetMaxIterations() const;
	void SetExpectMaxIterations(int expectedIterations_);

	OpType op;
	bool distinct;
	string name;

protected:
	bool nextUnsorted();

	bool nextFwd(IdType minHint);
	bool nextRev(IdType minHint);
	bool nextFwdSingleRange(IdType minHint);
	bool nextFwdSingleIdset(IdType minHint);
	bool nextRevSingleRange(IdType minHint);
	bool nextRevSingleIdset(IdType minHint);
	bool is_unsorted = false;

	bool reverse_ = false;
	bool forcedFirst_;
	int type_;
	IdType lastVal_ = INT_MIN;
	iterator lastIt_ = nullptr;
	IdType end_ = 0;
	int matchedCount_ = 0;
	int counter_ = 0;
};

}  // namespace reindexer
