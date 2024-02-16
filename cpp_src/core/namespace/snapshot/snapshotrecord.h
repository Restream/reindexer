#pragma once

#include "tools/lsn.h"
#include "tools/serializer.h"
#include "wal/walrecord.h"

namespace reindexer {

enum SnapshotRecordOpts {
	kShallowSnapshotChunk = 1 << 0,
	kWALSnapshotChunk = 1 << 1,
	kTxSnapshotChunk = 1 << 2,
	kLastSnapshotChunk = 1 << 3
};

class SnapshotRecord {
public:
	SnapshotRecord() = default;
	SnapshotRecord(lsn_t lsn, PackedWALRecord &&wrec) : lsn_(lsn), rec_(std::move(wrec)) {}
	void Deserialize(Serializer &ser);
	void Serilize(WrSerializer &ser) const;
	WALRecord Unpack() const { return WALRecord(rec_); }
	const PackedWALRecord &Record() const { return rec_; }
	lsn_t LSN() const noexcept { return lsn_; }

private:
	lsn_t lsn_ = lsn_t();
	PackedWALRecord rec_;
};

class SnapshotChunk {
public:
	const std::vector<SnapshotRecord> &Records() const noexcept { return records; }

	void Deserialize(Serializer &ser);
	void Serilize(WrSerializer &ser) const;

	void MarkShallow(bool v = true) noexcept { opts = v ? opts | kShallowSnapshotChunk : opts & ~(kShallowSnapshotChunk); }
	void MarkWAL(bool v = true) noexcept { opts = v ? opts | kWALSnapshotChunk : opts & ~(kWALSnapshotChunk); }
	void MarkTx(bool v = true) noexcept { opts = v ? opts | kTxSnapshotChunk : opts & ~(kTxSnapshotChunk); }
	void MarkLast(bool v = true) noexcept { opts = v ? opts | kLastSnapshotChunk : opts & ~(kLastSnapshotChunk); }

	bool IsShallow() const noexcept { return opts & kShallowSnapshotChunk; }
	bool IsWAL() const noexcept { return opts & kWALSnapshotChunk; }
	bool IsTx() const noexcept { return opts & kTxSnapshotChunk; }
	bool IsLastChunk() const noexcept { return opts & kLastSnapshotChunk; }

	std::vector<SnapshotRecord> records;
	uint16_t opts = 0;
};

struct SnapshotOpts {
	explicit SnapshotOpts(ExtendedLsn _from = ExtendedLsn(), int64_t _maxWalDepthOnForceSync = -1)
		: from(_from), maxWalDepthOnForceSync(_maxWalDepthOnForceSync) {}

	Error FromJSON(const gason::JsonNode &root);
	Error FromJSON(span<char> json);
	void GetJSON(JsonBuilder &jb) const;
	void GetJSON(WrSerializer &ser) const;

	ExtendedLsn from;
	int64_t maxWalDepthOnForceSync;
};

}  // namespace reindexer
