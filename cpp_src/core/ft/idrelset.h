#pragma once

#include <limits.h>
#include <algorithm>
#include <map>
#include <set>
#include "estl/h_vector.h"
#include "estl/packed_vector.h"
#include "sort/pdqsort.hpp"
#include "usingcontainer.h"

namespace reindexer {

typedef uint32_t VDocIdType;

// the position of the word in the document (the index of the word in the field (pos), the field in which the word field was
// encountered (field)
class IdRelType {
public:
	explicit IdRelType(VDocIdType id = 0) noexcept : id_(id) {}
	IdRelType(IdRelType&&) noexcept = default;
	IdRelType(const IdRelType&) = default;
	IdRelType& operator=(IdRelType&&) noexcept = default;
	IdRelType& operator=(const IdRelType&) = default;

	VDocIdType Id() const noexcept { return id_; }

	int Distance(const IdRelType& other, int max) const;

	int WordsInField(int field) const noexcept;
	int MinPositionInField(int field) const noexcept;
	// packed_vector callbacks
	size_t pack(uint8_t* buf) const;
	size_t unpack(const uint8_t* buf, unsigned len);
	size_t maxpackedsize() const { return 2 * (sizeof(VDocIdType) + 1) + (pos_.size() * (sizeof(uint32_t) + 1)); }

	void reserve(int s) { pos_.reserve(s); }
	bool empty() const noexcept { return pos_.empty(); }
	struct PosType {
		static const int posBits = 24;
		PosType() = default;
		PosType(int pos, int field) : fpos(pos | (field << posBits)) {}
		int pos() const noexcept { return fpos & ((1 << posBits) - 1); }
		int field() const noexcept { return fpos >> posBits; }
		bool operator<(PosType other) const noexcept { return fpos < other.fpos; }
		bool operator==(PosType other) const noexcept { return fpos == other.fpos; }
		unsigned fpos;
	};

	template <typename PosType>
	int MergeWithDist(const IdRelType& newWordPos, unsigned int dist, PosType& res) const {
		int minDist = INT_MAX;
		auto rightIt = newWordPos.pos_.begin();
		const auto leftEnd = pos_.end();
		const auto rightEnd = newWordPos.pos_.end();
		for (auto leftIt = pos_.begin(); leftIt != leftEnd; ++leftIt) {
			while (rightIt != rightEnd && rightIt->fpos < leftIt->fpos) {
				++rightIt;
			}
			// here right pos > left pos
			if (rightIt == rightEnd) {
				break;
			}
			if (rightIt->field() != leftIt->field()) {
				continue;
			}

			auto leftItNext = leftIt + 1;
			uint32_t leftNextPos = std::numeric_limits<uint32_t>::max();
			if (leftItNext != leftEnd) {
				leftNextPos = leftItNext->pos();
			}

			while (rightIt != rightEnd && rightIt->field() == leftIt->field() && uint32_t(rightIt->pos()) < leftNextPos &&
				   rightIt->fpos - leftIt->fpos <= dist) {
				int d = rightIt->fpos - leftIt->fpos;
				if (d < minDist) {
					minDist = d;
				}
				if constexpr (std::is_same_v<PosType, IdRelType>) {
					res.Add(*rightIt);
				} else {
					res.emplace_back(*rightIt, leftIt - pos_.begin());
				}
				++rightIt;
			}
		}
		return minDist;
	}

	void Add(int pos, int field) {
		pos_.emplace_back(pos, field);
		addField(field);
	}
	void Add(PosType p) {
		pos_.emplace_back(p);
		addField(p.field());
	}
	void SortAndUnique() {
		boost::sort::pdqsort(pos_.begin(), pos_.end());
		auto last = std::unique(pos_.begin(), pos_.end());
		pos_.resize(last - pos_.begin());
	}
	void Clear() {
		usedFieldsMask_ = 0;
#ifdef REINDEXER_FT_EXTRA_DEBUG
		pos_.clear<false>();
#else
		pos_.clear();
#endif
	}
	size_t Size() const noexcept { return pos_.size(); }
	size_t size() const noexcept { return pos_.size(); }
	void SimpleCommit();
	const RVector<PosType, 3>& Pos() const noexcept { return pos_; }
	uint64_t UsedFieldsMask() const noexcept { return usedFieldsMask_; }
	size_t HeapSize() const noexcept { return heapSize(pos_); }

private:
	static constexpr int maxField = 63;

	void addField(int field) noexcept {
		assertrx(0 <= field && field <= maxField);
		usedFieldsMask_ |= (uint64_t(1) << field);
	}

	template <typename T>
	size_t heapSize(const T& p) const noexcept {
		if constexpr (std::is_same_v<T, PosType>) {
			return p.heap_size();
		} else {
			return p.capacity() * sizeof(PosType);
		}
	}

	RVector<PosType, 3> pos_;
	uint64_t usedFieldsMask_ = 0;  // fields that occur in pos_
	VDocIdType id_ = 0;			   // index of the document in which the word occurs
};

class IdRelSet : public std::vector<IdRelType> {
public:
	int Add(VDocIdType id, int pos, int field);
	void SimpleCommit() {
		for (auto& val : *this) val.SimpleCommit();
	}

	VDocIdType max_id_ = 0;
	VDocIdType min_id_ = INT_MAX;
};

using PackedIdRelVec = packed_vector<IdRelType>;

class IdRelVec : public std::vector<IdRelType> {
public:
	size_t heap_size() const noexcept {
		size_t res = capacity() * sizeof(IdRelType);
		for (const auto& id : *this) res += id.HeapSize();
		return res;
	}
	void erase_back(size_t pos) noexcept { erase(begin() + pos, end()); }
	size_t pos(const_iterator it) const noexcept { return it - cbegin(); }
};

}  // namespace reindexer
