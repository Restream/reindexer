#pragma once

#include "estl/defines.h"
#include "estl/h_vector.h"
#include "sort/pdqsort.hpp"

namespace reindexer {

struct [[nodiscard]] Area {
public:
	Area() noexcept = default;
	Area(unsigned s, unsigned e, unsigned idx) noexcept : start(s), end(e), arrayIdx(idx) {}

	bool Concat(const Area& rhs) noexcept {
		if (arrayIdx != rhs.arrayIdx) {
			return false;
		}

		if (isIn(rhs.start) || isIn(rhs.end) || (start > rhs.start && end < rhs.end)) {
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

	unsigned start = 0;
	unsigned end = 0;
	unsigned arrayIdx = 0;

private:
	bool inline isIn(unsigned pos) noexcept { return pos <= end && pos >= start; }
};

struct [[nodiscard]] AreaDebug {
	enum class [[nodiscard]] PhraseMode { None, Start, End };
	AreaDebug() = default;
	AreaDebug(unsigned s, unsigned e, unsigned idx, std::string&& p, PhraseMode phMode) noexcept
		: start(s), end(e), arrayIdx(idx), props(p), phraseMode(phMode) {}
	RX_ALWAYS_INLINE bool Concat(const AreaDebug&) noexcept { return false; }
	unsigned start = 0;
	unsigned end = 0;
	unsigned arrayIdx = 0;
	std::string props;
	PhraseMode phraseMode = PhraseMode::None;
};

template <typename AreaType>
class AreasInDocument;

template <typename AreaType>
class [[nodiscard]] AreasInField {
public:
	AreasInField() = default;

	size_t Size() const noexcept { return data_.size(); }

	bool Empty() const noexcept { return data_.empty(); }

	void Commit() {
		if (!data_.empty()) {
			boost::sort::pdqsort_branchless(data_.begin(), data_.end(),
											[](const AreaType& rhs, const AreaType& lhs) noexcept { return rhs.start < lhs.start; });
			for (auto vit = data_.begin() + 1; vit != data_.end(); ++vit) {
				auto prev = vit - 1;
				if (vit->Concat(*prev)) {
					vit = data_.erase(prev);
				}
			}
		}
	}

	bool Insert(AreaType&& area, float termRank, unsigned maxAreasInDoc, float maxTermRank) {
		if (index_ > 0 && data_[(index_ - 1) % maxAreasInDoc].Concat(area)) {
			return true;
		} else {
			if (maxAreasInDoc > 0 && data_.size() == maxAreasInDoc) {
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

	const h_vector<AreaType, 2>& GetData() const noexcept { return data_; }
	void MoveAreas(AreasInDocument<AreaType>& to, unsigned field, float rank, unsigned maxAreasInDoc) {
		for (auto& v : data_) {
			[[maybe_unused]] bool r = to.InsertArea(std::move(v), field, rank, maxAreasInDoc);
		}
		to.UpdateRank(rank);
		data_.resize(0);
	}

private:
	h_vector<AreaType, 2> data_;
	int index_ = 0;
};

template <typename AreaType>
class [[nodiscard]] AreasInDocument {
public:
	AreasInDocument() = default;
	~AreasInDocument() = default;
	AreasInDocument(AreasInDocument&&) = default;

	void Reserve(int size) { areas_.reserve(size); }
	void ReserveField(int size) { areas_.resize(size); }
	void Commit() {
		committed_ = true;
		for (auto& area : areas_) {
			area.Commit();
		}
	}
	bool AddWord(AreaType&& area, unsigned field, float rank, int maxAreasInDoc) {
		return InsertArea(std::move(area), field, rank, maxAreasInDoc);
	}
	void UpdateRank(float rank) noexcept {
		if (rank > maxTermRank_) {
			maxTermRank_ = rank;
		}
	}

	AreasInField<AreaType>* GetAreas(unsigned field) {
		if (!committed_) {
			Commit();
		}
		return (areas_.size() <= field) ? nullptr : &areas_[field];
	}
	AreasInField<AreaType>* GetAreasRaw(unsigned field) noexcept { return (areas_.size() <= field) ? nullptr : &areas_[field]; }
	bool IsCommitted() const noexcept { return committed_; }
	size_t GetAreasCount() const noexcept {
		size_t size = 0;
		for (const auto& aVec : areas_) {
			size += aVec.Size();
		}
		return size;
	}
	bool InsertArea(AreaType&& area, unsigned field, float rank, int maxAreasInDoc) {
		committed_ = false;
		if (areas_.size() <= field) {
			areas_.resize(field + 1);
		}
		auto& fieldAreas = areas_[field];
		return fieldAreas.Insert(std::move(area), rank, maxAreasInDoc, maxTermRank_);
	}

private:
	bool committed_ = false;
	h_vector<AreasInField<AreaType>, 3> areas_;
	float maxTermRank_ = 0;
};

}  // namespace reindexer
