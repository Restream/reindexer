#pragma once
#include <algorithm>
#include <iostream>
#include <set>
#include <vector>
#include "estl/fast_hash_map.h"

namespace reindexer {
using namespace std;
struct Area {
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
		if (IsIn(rhs.start_) || IsIn(rhs.end_) || (start_ > rhs.start_ && end_ < rhs.end_)) {
			if (start_ > rhs.start_) start_ = rhs.start_;
			if (end_ < rhs.end_) end_ = rhs.end_;
			return true;
		}
		return false;
	}
	int start_;
	int end_;
};

class AreaHolder {
public:
	typedef fast_hash_map<int, std::vector<Area>> area_type;
	typedef shared_ptr<AreaHolder> Ptr;
	typedef unique_ptr<AreaHolder> UniquePtr;

	AreaHolder(int buffer_size, int total_size, int space_size)
		: buffer_size_(buffer_size), total_size_(total_size), space_size_(space_size) {}
	AreaHolder() {}
	void Reserve(int size);
	void ReserveField(int size);
	void AddTreeGramm(int pos, int filed);
	void Commit();
	int GetSize() { return total_size_; }
	void AddWord(int start_pos, int size, int filed);
	vector<Area> GetSnippet(int field, int front, int back, int total_size);
	vector<Area> *GetAreas(int field);

private:
	void insertArea(const Area &area, int filed);
	int buffer_size_;
	int total_size_;
	int space_size_;
	vector<vector<Area>> areas;
};
}  // namespace reindexer
