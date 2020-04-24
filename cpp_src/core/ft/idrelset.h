#pragma once

#include <limits.h>
#include <algorithm>
#include "estl/h_vector.h"
#include "estl/packed_vector.h"
namespace reindexer {

typedef uint32_t VDocIdType;

class IdRelType {
public:
	// Disable copy - enable only move
	explicit IdRelType(VDocIdType id = 0) : id_(id) {}
	IdRelType(IdRelType&&) noexcept = default;
	IdRelType(const IdRelType&) = delete;
	IdRelType& operator=(IdRelType&&) noexcept = default;
	IdRelType& operator=(const IdRelType&) = delete;

	VDocIdType Id() const noexcept { return id_; }

	int Distance(const IdRelType& other, int max) const;

	int WordsInField(int field);
	// packed_vector callbacks
	size_t pack(uint8_t* buf) const;
	size_t unpack(const uint8_t* buf, unsigned len);
	size_t maxpackedsize() const { return 2 * (sizeof(VDocIdType) + 1) + (pos_.size() * (sizeof(uint32_t) + 1)); }

	struct PosType {
		static const int posBits = 24;
		PosType() = default;
		PosType(int pos, int field) : fpos(pos | (field << posBits)) {}
		int pos() const { return fpos & ((1 << posBits) - 1); }
		int field() const { return fpos >> posBits; }
		unsigned fpos;
	};

	void Add(int pos, int field) {
		pos_.emplace_back(pos, field);
		addField(field);
	}
	size_t Size() const noexcept { return pos_.size(); }
	void SimpleCommit();
	const h_vector<PosType, 3>& Pos() const { return pos_; }
	uint64_t UsedFieldsMask() const noexcept { return usedFieldsMask_; }

private:
	static constexpr int maxField = 63;

	void addField(int field) noexcept {
		assert(0 <= field && field <= maxField);
		usedFieldsMask_ |= (uint64_t(1) << field);
	}

	h_vector<PosType, 3> pos_;
	uint64_t usedFieldsMask_ = 0;
	VDocIdType id_ = 0;
};

class IdRelSet : public h_vector<IdRelType, 0> {
public:
	int Add(VDocIdType id, int pos, int field);
	void SimpleCommit() {
		for (auto& val : *this) val.SimpleCommit();
	}

	VDocIdType max_id_ = 0;
	VDocIdType min_id_ = INT_MAX;
};

using PackedIdRelSet = packed_vector<IdRelType>;

}  // namespace reindexer
