#pragma once
#include <algorithm>
#include <iostream>
#include <set>
#include <vector>
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"

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

using AreaVec = h_vector<Area, 2>;
class AreaHolder {
public:
	typedef std::shared_ptr<AreaHolder> Ptr;
	typedef std::unique_ptr<AreaHolder> UniquePtr;

	AreaHolder(int buffer_size, int total_size, int space_size)
		: buffer_size_(buffer_size), total_size_(total_size), space_size_(space_size), commited_(false) {}
	AreaHolder() = default;
	~AreaHolder() = default;
	void Reserve(int size);
	void ReserveField(int size);
	void AddTreeGramm(int pos, int filed, int maxAreasInDoc);
	void Commit();
	int GetSize() { return total_size_; }
	bool AddWord(int start_pos, int size, int filed, int maxAreasInDoc);
	AreaVec *GetAreas(int field);
	bool IsCommited() const noexcept { return commited_; }
	size_t GetAreasCount() const noexcept {
		size_t size = 0;
		for (const auto &aVec : areas) {
			size += aVec.size();
		}
		return size;
	}

private:
	bool insertArea(Area &&area, int filed, int maxAreasInDoc);
	int buffer_size_ = 0;
	int total_size_ = 0;
	int space_size_ = 0;
	bool commited_ = false;
	h_vector<AreaVec, 3> areas;
};
}  // namespace reindexer
