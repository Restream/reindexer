#pragma once

#include <limits.h>
#include <algorithm>
#include "estl/h_vector.h"
#include "estl/packed_vector.h"
#include "sort/pdqsort.hpp"

namespace reindexer {

typedef uint32_t VDocIdType;
static constexpr int kMaxFtCompositeFields = 63;

// the position of the word in the document (the index of the word in the field (pos), the field in which the word field was
// encountered (field)
class [[nodiscard]] IdRelType {
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
	struct [[nodiscard]] PosType {
		static const int posBits = 24;
		PosType() = default;
		PosType(int pos, int field) noexcept : fpos(pos | (field << posBits)) {}
		int pos() const noexcept { return fpos & ((1 << posBits) - 1); }
		int field() const noexcept { return fpos >> posBits; }
		bool operator<(PosType other) const noexcept { return fpos < other.fpos; }
		bool operator==(PosType other) const noexcept { return fpos == other.fpos; }

		uint32_t fpos;
	};

	template <typename PosTypeT>
	int MergeWithDist(const IdRelType& newWordPos, unsigned int dist, PosTypeT& res, const std::string& inf) const;

	void Add(int pos, int field) {
		assertrx_throw(0 <= field && field <= kMaxFtCompositeFields);
		pos_.emplace_back(pos, field);
		addField(field);
	}
	void Add(PosType p) {
		const auto field = p.field();
		assertrx_throw(0 <= field && field <= kMaxFtCompositeFields);
		pos_.emplace_back(p);
		addField(field);
	}
	void SortAndUnique() {
		boost::sort::pdqsort_branchless(pos_.begin(), pos_.end());
		auto last = std::unique(pos_.begin(), pos_.end());
		pos_.resize(last - pos_.begin());
	}
	void Clear() noexcept {
		usedFieldsMask_ = 0;
#ifdef REINDEXER_FT_EXTRA_DEBUG
		pos_.clear<false>();
#else
		pos_.clear();
#endif
	}
	size_t Size() const noexcept { return pos_.size(); }
	size_t size() const noexcept { return pos_.size(); }
	void SimpleCommit() noexcept {
		boost::sort::pdqsort_branchless(
			pos_.begin(), pos_.end(),
			[](const IdRelType::PosType& lhs, const IdRelType::PosType& rhs) noexcept { return lhs.pos() < rhs.pos(); });
	}
	const h_vector<PosType, 3>& Pos() const noexcept { return pos_; }
	uint64_t UsedFieldsMask() const noexcept { return usedFieldsMask_; }
	size_t HeapSize() const noexcept { return heapSize(pos_); }

private:
	void addField(int field) noexcept { usedFieldsMask_ |= (uint64_t(1) << field); }

	template <typename T>
	size_t heapSize(const T& p) const noexcept {
		if constexpr (std::is_same_v<T, PosType>) {
			return p.heap_size();
		} else {
			return p.capacity() * sizeof(PosType);
		}
	}

	h_vector<PosType, 3> pos_;
	uint64_t usedFieldsMask_ = 0;  // fields that occur in pos_
	VDocIdType id_ = 0;			   // index of the document in which the word occurs
};

struct [[nodiscard]] PosTypeDebug : public IdRelType::PosType {
	PosTypeDebug() = default;
	explicit PosTypeDebug(const IdRelType::PosType& pos, const std::string& inf) : IdRelType::PosType(pos), info(inf) {}
	explicit PosTypeDebug(const IdRelType::PosType& pos, std::string&& inf) noexcept : IdRelType::PosType(pos), info(std::move(inf)) {}
	std::string info;
};

class [[nodiscard]] IdRelSet : public std::vector<IdRelType> {
public:
	int Add(VDocIdType id, int pos, int field) {
		if (id > max_id_) {
			max_id_ = id;
		}
		if (id < min_id_) {
			min_id_ = id;
		}

		auto& last = (empty() || back().Id() != id) ? emplace_back(id) : back();
		last.Add(pos, field);
		return last.size();
	}
	void SimpleCommit() noexcept {
		for (auto& val : *this) {
			val.SimpleCommit();
		}
	}

	VDocIdType max_id_ = 0;
	VDocIdType min_id_ = INT_MAX;
};

using PackedIdRelVec = packed_vector<IdRelType>;

class [[nodiscard]] IdRelVec : public std::vector<IdRelType> {
public:
	size_t heap_size() const noexcept {
		size_t res = capacity() * sizeof(IdRelType);
		for (const auto& id : *this) {
			res += id.HeapSize();
		}
		return res;
	}
	void erase_back(size_t pos) noexcept { erase(begin() + pos, end()); }
	size_t pos(const_iterator it) const noexcept { return it - cbegin(); }
};

}  // namespace reindexer
