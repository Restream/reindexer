#pragma once
#include <algorithm>
#include <iostream>
#include <set>
#include <vector>
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "usingcontainer.h"

namespace reindexer {

struct Area {
	Area() : start_(0), end_(0) {}
	Area(int start, int end) : start_(start), end_(end) {}

	bool inline IsIn(int pos, bool exect = false) {
		if (exect) {
			if (pos <= end_ && pos >= start_) return true;
		} else {
			if (pos <= end_ + 1 && pos >= start_ - 1) return true;
		}
		return false;
	}

	bool inline Concat(const Area &rhs) {
		if (IsIn(rhs.start_, true) || IsIn(rhs.end_, true) || (start_ > rhs.start_ && end_ < rhs.end_)) {
			if (start_ > rhs.start_) start_ = rhs.start_;
			if (end_ < rhs.end_) end_ = rhs.end_;
			return true;
		}
		return false;
	}
	int start_;
	int end_;
};

using AreaVec = RVector<Area, 2>;
class AreaHolder {
public:
	typedef std::shared_ptr<AreaHolder> Ptr;
	typedef std::unique_ptr<AreaHolder> UniquePtr;

	AreaHolder() = default;
	~AreaHolder() = default;
	void Reserve(int size);
	void ReserveField(int size);
	void Commit();
	bool AddWord(int pos, int filed, int maxAreasInDoc);
	AreaVec *GetAreas(int field);
	AreaVec *GetAreasRaw(int field);
	bool IsCommited() const noexcept { return commited_; }
	size_t GetAreasCount() const noexcept {
		size_t size = 0;
		for (const auto &aVec : areas) {
			size += aVec.size();
		}
		return size;
	}
	bool InsertArea(Area &&area, int filed, int maxAreasInDoc);

private:
	bool commited_ = false;
	RVector<AreaVec, 3> areas;
};
}  // namespace reindexer
