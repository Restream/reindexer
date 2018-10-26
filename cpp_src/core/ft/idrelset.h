#pragma once

#include <limits.h>
#include <algorithm>
#include "estl/h_vector.h"
#include "estl/packed_vector.h"
namespace reindexer {

typedef uint32_t VDocIdType;

struct IdRelType {
	// Disable copy - enable only move
	IdRelType() = default;
	IdRelType(IdRelType&&) noexcept = default;
	IdRelType(const IdRelType&) = delete;
	IdRelType& operator=(IdRelType&&) noexcept = default;
	IdRelType& operator=(const IdRelType&) = delete;

	int pos2rank(int pos) const {
		if (pos <= 10) return 100 - pos;
		if (pos <= 100) return 90 - (pos / 10);
		if (pos <= 1000) return 80 - (pos / 100);
		return 70;
	}

	int rank() const { return !pos.size() ? 0 : pos2rank(pos.front().pos()) + std::max(10, int(pos.size())); }

	int distance(const IdRelType& other, int max) const;

	int wordsInField(int field);
	// packed_vector callbacks
	size_t pack(uint8_t* buf) const;
	size_t unpack(const uint8_t* buf, unsigned len);
	size_t maxpackedsize() const { return 2 * (sizeof(VDocIdType) + 1) + (pos.size() * (sizeof(uint32_t) + 1)); }

	struct PosType {
		static const int posBits = 24;
		PosType() = default;
		PosType(int pos, int field) : fpos(pos | (field << posBits)) {}
		int pos() const { return fpos & ((1 << posBits) - 1); }
		int field() const { return fpos >> posBits; }
		unsigned fpos;
	};

	h_vector<PosType, 3> pos;
	VDocIdType id = 0;
};

class IdRelSet : public h_vector<IdRelType, 0> {
public:
	int Add(VDocIdType id, int pos, int field);
	void Commit();
	void SimpleCommit();

	VDocIdType max_id_ = 0;
	VDocIdType min_id_ = INT_MAX;
};

using PackedIdRelSet = packed_vector<IdRelType>;

}  // namespace reindexer
