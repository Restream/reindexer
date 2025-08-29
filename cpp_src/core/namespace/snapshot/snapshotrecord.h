#pragma once

#include "tools/lsn.h"
#include "tools/serializer.h"
#include "wal/walrecord.h"

namespace reindexer {

enum [[nodiscard]] SnapshotRecordOpts {
	kShallowSnapshotChunk = 1 << 0,
	kWALSnapshotChunk = 1 << 1,
	kTxSnapshotChunk = 1 << 2,
	kLastSnapshotChunk = 1 << 3
};

class [[nodiscard]] SnapshotRecord {
public:
	SnapshotRecord() = default;
	SnapshotRecord(lsn_t lsn, PackedWALRecord&& wrec) : lsn_(lsn), rec_(std::move(wrec)) {}
	void Deserialize(Serializer& ser);
	void Serilize(WrSerializer& ser) const;
	WALRecord Unpack() const { return WALRecord(rec_); }
	const PackedWALRecord& Record() const { return rec_; }
	lsn_t LSN() const noexcept { return lsn_; }

private:
	lsn_t lsn_ = lsn_t();
	PackedWALRecord rec_;
};

class [[nodiscard]] SnapshotChunk {
public:
	const std::vector<SnapshotRecord>& Records() const noexcept { return records; }

	void Deserialize(Serializer& ser);
	void Serilize(WrSerializer& ser) const;

	void MarkShallow(bool v = true) noexcept { opts_ = v ? opts_ | kShallowSnapshotChunk : opts_ & ~(kShallowSnapshotChunk); }
	void MarkWAL(bool v = true) noexcept { opts_ = v ? opts_ | kWALSnapshotChunk : opts_ & ~(kWALSnapshotChunk); }
	void MarkTx(bool v = true) noexcept { opts_ = v ? opts_ | kTxSnapshotChunk : opts_ & ~(kTxSnapshotChunk); }
	void MarkLast(bool v = true) noexcept { opts_ = v ? opts_ | kLastSnapshotChunk : opts_ & ~(kLastSnapshotChunk); }

	bool IsShallow() const noexcept { return opts_ & kShallowSnapshotChunk; }
	bool IsWAL() const noexcept { return opts_ & kWALSnapshotChunk; }
	bool IsTx() const noexcept { return opts_ & kTxSnapshotChunk; }
	bool IsLastChunk() const noexcept { return opts_ & kLastSnapshotChunk; }

	std::vector<SnapshotRecord> records;

private:
	uint16_t opts_ = 0;
};

enum [[nodiscard]] SnapshotOptsEnum {
	kSnapshotWithDataCount = 1 << 0,
	kSnapshotWithClusterStatus = 1 << 1,
};

struct [[nodiscard]] SnapshotOpts {
	explicit SnapshotOpts(ExtendedLsn _from = ExtendedLsn(), int64_t _maxWalDepthOnForceSync = -1) noexcept
		: from(_from), maxWalDepthOnForceSync(_maxWalDepthOnForceSync) {}

	Error FromJSON(const gason::JsonNode& root);
	Error FromJSON(std::span<char> json);
	void GetJSON(JsonBuilder& jb) const;
	void GetJSON(WrSerializer& ser) const;

	ExtendedLsn from;
	int64_t maxWalDepthOnForceSync;
};

}  // namespace reindexer
