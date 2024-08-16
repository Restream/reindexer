#pragma once
#include <algorithm>
#include <iostream>
#include <set>
#include <vector>
#include "estl/h_vector.h"
#include "sort/pdqsort.hpp"
#include "usingcontainer.h"

namespace reindexer {

struct Area {
public:
	Area() noexcept : start(0), end(0) {}
	Area(int s, int e) noexcept : start(s), end(e) {}

	[[nodiscard]] bool inline IsIn(int pos) noexcept { return pos <= end && pos >= start; }

	[[nodiscard]] bool inline Concat(const Area& rhs) noexcept {
		if (IsIn(rhs.start) || IsIn(rhs.end) || (start > rhs.start && end < rhs.end)) {
			if (start > rhs.start) {
				start = rhs.start;
			}
			if (end < rhs.end) {
				end = rhs.end;
			}
			return true;
		}
		return false;
	}

	int start;
	int end;
};

class AreaHolder;

class AreaBuffer {
public:
	AreaBuffer() = default;
	[[nodiscard]] size_t Size() const noexcept { return data_.size(); }
	[[nodiscard]] bool Empty() const noexcept { return data_.empty(); }
	void Commit() {
		if (!data_.empty()) {
			boost::sort::pdqsort_branchless(data_.begin(), data_.end(),
											[](const Area& rhs, const Area& lhs) noexcept { return rhs.start < lhs.start; });
			for (auto vit = data_.begin() + 1; vit != data_.end(); ++vit) {
				auto prev = vit - 1;
				if (vit->Concat(*prev)) {
					vit = data_.erase(prev);
				}
			}
		}
	}
	[[nodiscard]] bool Insert(Area&& area, float termRank, int maxAreasInDoc, float maxTermRank) {
		if (!data_.empty() && data_.back().Concat(area)) {
			return true;
		} else {
			if (maxAreasInDoc > 0 && data_.size() == unsigned(maxAreasInDoc)) {
				if (termRank > maxTermRank) {
					data_[index_ % maxAreasInDoc] = std::move(area);
					index_++;
					return true;
				}
			} else {
				data_.emplace_back(std::move(area));
				index_++;
				return true;
			}
		}
		return false;
	}

	[[nodiscard]] const RVector<Area, 2>& GetData() const noexcept { return data_; }
	void MoveAreas(AreaHolder& to, int field, int32_t rank, int maxAreasInDoc);

private:
	RVector<Area, 2> data_;
	int index_ = 0;
};

class AreaHolder {
public:
	typedef std::shared_ptr<AreaHolder> Ptr;
	typedef std::unique_ptr<AreaHolder> UniquePtr;

	AreaHolder() = default;
	~AreaHolder() = default;
	AreaHolder(AreaHolder&&) = default;
	void Reserve(int size) { areas_.reserve(size); }
	void ReserveField(int size) { areas_.resize(size); }
	void Commit() {
		commited_ = true;
		for (auto& area : areas_) {
			area.Commit();
		}
	}
	[[nodiscard]] bool AddWord(int pos, int field, int32_t rank, int maxAreasInDoc) {
		return InsertArea(Area{pos, pos + 1}, field, rank, maxAreasInDoc);
	}
	void UpdateRank(int32_t rank) noexcept {
		if (rank > maxTermRank_) {
			maxTermRank_ = rank;
		}
	}
	[[nodiscard]] AreaBuffer* GetAreas(int field) {
		if (!commited_) {
			Commit();
		}
		return (areas_.size() <= size_t(field)) ? nullptr : &areas_[field];
	}
	[[nodiscard]] AreaBuffer* GetAreasRaw(int field) noexcept { return (areas_.size() <= size_t(field)) ? nullptr : &areas_[field]; }
	[[nodiscard]] bool IsCommited() const noexcept { return commited_; }
	[[nodiscard]] size_t GetAreasCount() const noexcept {
		size_t size = 0;
		for (const auto& aVec : areas_) {
			size += aVec.Size();
		}
		return size;
	}
	[[nodiscard]] bool InsertArea(Area&& area, int field, int32_t rank, int maxAreasInDoc) {
		commited_ = false;
		if (areas_.size() <= size_t(field)) {
			areas_.resize(field + 1);
		}
		auto& fieldAreas = areas_[field];
		return fieldAreas.Insert(std::move(area), rank, maxAreasInDoc, maxTermRank_);
	}

private:
	bool commited_ = false;
	RVector<AreaBuffer, 3> areas_;
	int32_t maxTermRank_ = 0;
};

inline void AreaBuffer::MoveAreas(AreaHolder& to, int field, int32_t rank, int maxAreasInDoc) {
	for (auto& v : data_) {
		[[maybe_unused]] bool r = to.InsertArea(std::move(v), field, rank, maxAreasInDoc);
	}
	to.UpdateRank(rank);
	data_.resize(0);
}

}  // namespace reindexer
