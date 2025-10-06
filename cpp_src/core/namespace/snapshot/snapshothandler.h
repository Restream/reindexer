#pragma once

#include "snapshot.h"
#include "updates/updaterecord.h"

namespace reindexer {

class NamespaceImpl;
class Namespace;

class [[nodiscard]] SnapshotHandler {
public:
	SnapshotHandler(NamespaceImpl& ns) : ns_(ns) {}

	Snapshot CreateSnapshot(const SnapshotOpts& opts) const;
	void ApplyChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, UpdatesContainer& repl);

private:
	struct [[nodiscard]] ChunkContext {
		bool wal = false;
		bool shallow = false;
		bool tx = false;
		bool initialLeaderSync = false;
	};

	void applyRecord(const SnapshotRecord& rec, const ChunkContext& ctx, UpdatesContainer& repl);
	void applyShallowRecord(lsn_t lsn, WALRecType type, const PackedWALRecord& wrec, const ChunkContext& chCtx);
	void applyRealRecord(lsn_t lsn, const SnapshotRecord& snRec, const ChunkContext& chCtx, UpdatesContainer& repl);

	NamespaceImpl& ns_;
	RdxContext dummyCtx_;
};

class [[nodiscard]] SnapshotTxHandler {
public:
	SnapshotTxHandler(Namespace& ns) : ns_(ns) {}
	void ApplyChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, const RdxContext& rdxCtx);

private:
	Namespace& ns_;
};

}  // namespace reindexer
