#include "snapshot.h"
#include <sstream>
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsonbuilder.h"

namespace reindexer {

static constexpr size_t kDefaultChunkSize = 500;

Snapshot::Snapshot(TagsMatcher tm, lsn_t nsVersion, uint64_t expectedDataHash, uint64_t expectedDataCount,
				   ClusterOperationStatus clusterStatus)
	: tm_(std::move(tm)),
	  expectedDataHash_(expectedDataHash),
	  expectedDataCount_(expectedDataCount),
	  clusterOperationStatus_(std::move(clusterStatus)),
	  nsVersion_(nsVersion) {
	walData_.AddItem(ItemRef(-1, createTmItem(), 0, true));
}

Snapshot::Snapshot(PayloadType pt, TagsMatcher tm, lsn_t nsVersion, lsn_t lastLsn, uint64_t expectedDataHash, uint64_t expectedDataCount,
				   ClusterOperationStatus clusterStatus, LocalQueryResults&& wal, LocalQueryResults&& raw)
	: pt_(std::move(pt)),
	  tm_(std::move(tm)),
	  expectedDataHash_(expectedDataHash),
	  expectedDataCount_(expectedDataCount),
	  clusterOperationStatus_(std::move(clusterStatus)),
	  lastLsn_(lastLsn),
	  nsVersion_(nsVersion) {
	if (raw.Items().Size()) {
		rawData_.AddItem(ItemRef(-1, createTmItem(), 0, true));
	} else {
		walData_.AddItem(ItemRef(-1, createTmItem(), 0, true));
	}

	addRawData(std::move(raw));
	addWalData(std::move(wal));
	lockItems(true);
}

Snapshot& Snapshot::operator=(Snapshot&& other) noexcept {
	lockItems(false);
	pt_ = std::move(other.pt_);
	tm_ = std::move(other.tm_);
	rawData_ = std::move(other.rawData_);
	walData_ = std::move(other.walData_);
	expectedDataHash_ = other.expectedDataHash_;
	expectedDataCount_ = other.expectedDataCount_;
	clusterOperationStatus_ = other.clusterOperationStatus_;
	lastLsn_ = other.lastLsn_;
	nsVersion_ = other.nsVersion_;
	return *this;
}

Snapshot::~Snapshot() {
	// Unlock items
	lockItems(false);
}

std::string Snapshot::Dump() {
	std::stringstream ss;
	ss << fmt::format(
		"Snapshot:\nNs version: {}\nLast LSN: {}\nDatahash: {}\nDatacount: {}\nRaw data blocks: {}\nWAL data blocks: {}\nWAL data:",
		int64_t(nsVersion_), int64_t(lastLsn_), expectedDataHash_, expectedDataCount_, rawData_.Size(), walData_.Size());
	size_t chNum = 0;
	size_t itemNum = 0;
	WrSerializer ser;
	for (auto it = Iterator{this, rawData_.Size()}; it != end(); ++it) {
		auto ch = it.Chunk();
		for (auto& rec : ch.Records()) {
			ser.Reset();
			WALRecord wrec(rec.Record());
			wrec.Dump(ser, [](std::string_view) { return std::string("<cjson>"); });
			ss << fmt::format("{}.{} LSN: {}, type: {}, dump: {}\n", chNum, itemNum, int64_t(rec.LSN()), wrec.type, ser.Slice());
			++itemNum;
		}
		++chNum;
	}
	return ss.str();
}

void Snapshot::ItemsContainer::AddItem(ItemRef&& item) {
	bool batchRecord = data_.size() && data_.back().txChunk;
	bool requireNewChunk = data_.empty() || (data_.back().items.size() >= kDefaultChunkSize && !batchRecord);
	bool createEmptyChunk = false;
	if (item.Raw() && item.Value().GetCapacity()) {
		WALRecord rec(std::span<uint8_t>(item.Value().Ptr(), item.Value().GetCapacity()));
		if (rec.inTransaction != batchRecord) {
			requireNewChunk = true;
		}
		batchRecord = rec.inTransaction;
		if (rec.type == WalInitTransaction) {
			requireNewChunk = true;
		} else if (rec.type == WalCommitTransaction) {
			createEmptyChunk = true;
		}
	}
	if (data_.size() && data_.back().items.empty()) {
		data_.back().items.reserve(kDefaultChunkSize);
		data_.back().txChunk = batchRecord;
	} else if (requireNewChunk) {
		data_.emplace_back();
		data_.back().items.reserve(kDefaultChunkSize);
		data_.back().txChunk = batchRecord;
	}
	auto& chunk = data_.back().items;
	chunk.emplace_back(std::move(item));
	++itemsCount_;

	if (createEmptyChunk) {
		data_.emplace_back();
	}
}

void Snapshot::ItemsContainer::LockItems(const PayloadType& pt, bool lock) {
	for (auto& chunk : data_) {
		for (auto& item : chunk.items) {
			lockItem(pt, item, lock);
		}
	}
}

void Snapshot::ItemsContainer::lockItem(const PayloadType& pt, ItemRef& itemref, bool lock) noexcept {
	if (!itemref.Value().IsFree() && !itemref.Raw()) {
		Payload pl(pt, itemref.Value());
		if (lock) {
			pl.AddRefStrings();
		} else {
			pl.ReleaseStrings();
		}
	}
}

void Snapshot::addRawData(LocalQueryResults&& qr) {
	appendQr(rawData_, std::move(qr));

	if (rawData_.Size()) {
		WALRecord wrec(WalResetLocalWal);
		PackedWALRecord wr;
		wr.Pack(wrec);
		PayloadValue val(wr.size(), wr.data());
		val.SetLSN(lsn_t());
		rawData_.AddItem(ItemRef(-1, val, 0, true));
	}
}

void Snapshot::addWalData(LocalQueryResults&& qr) { appendQr(walData_, std::move(qr)); }

void Snapshot::appendQr(ItemsContainer& container, LocalQueryResults&& qr) {
	ItemRefVector& items = qr.Items();
	if (container.ItemsCount() > 1) {
		throw Error(errLogic, "Snapshot already has this kind of data");
	}
	for (const auto& it : items) {
		container.AddItem(std::move(it.GetItemRef()));
	}
	container.SetVectorsHolder(std::move(qr.GetFloatVectorsHolder()));
}

void Snapshot::lockItems(bool lock) {
	rawData_.LockItems(pt_, lock);
	walData_.LockItems(pt_, lock);
}

PayloadValue Snapshot::createTmItem() const {
	WrSerializer ser;
	ser.PutVarint(tm_.version());
	ser.PutVarint(tm_.stateToken());
	tm_.serialize(ser);
	WALRecord wrec(WalTagsMatcher, ser.Slice());
	PackedWALRecord wr;
	wr.Pack(wrec);
	PayloadValue val(wr.size(), wr.data());
	val.SetLSN(lsn_t());
	return val;
}

SnapshotChunk Snapshot::Iterator::Chunk() const {
	bool wal = false;
	size_t idx = idx_;
	if (idx >= sn_->Size()) {
		throw Error(errLogic, "Index out of range: {}", idx);
	}

	const auto* dataPtr = &sn_->rawData_;
	if (idx >= dataPtr->Size()) {
		wal = true;
		idx -= dataPtr->Size();
		dataPtr = &sn_->walData_;
	}
	const bool shallow = wal && sn_->rawData_.Size();
	const auto& chunks = dataPtr->Data();
	SnapshotChunk chunk;
	chunk.records.reserve(chunks[idx].items.size());
	for (auto& itemRef : chunks[idx].items) {
		PackedWALRecord pwrec;
		if (itemRef.Raw()) {
			pwrec.resize(itemRef.Value().GetCapacity());
			memcpy(pwrec.data(), itemRef.Value().Ptr(), pwrec.size());
		} else if (shallow) {
			assertrx(itemRef.Id() >= 0);
			pwrec.Pack(WALRecord(WalShallowItem, itemRef.Id()));
		} else {
			ser_.Reset();
			ConstPayload pl(sn_->pt_, itemRef.Value());
			CJsonBuilder builder(ser_, ObjType::TypePlain);
			CJsonEncoder cjsonEncoder(&sn_->tm_, nullptr);

			cjsonEncoder.Encode(pl, builder);
			pwrec.Pack(WALRecord(WalRawItem, itemRef.Id(), ser_.Slice()));
		}
		chunk.records.emplace_back(itemRef.Value().GetLSN(), std::move(pwrec));
	}
	chunk.MarkShallow(shallow);
	chunk.MarkWAL(wal);
	chunk.MarkTx(chunks[idx].txChunk);
	chunk.MarkLast(idx_ == sn_->Size() - 1);
	return chunk;
}

Snapshot::Iterator& Snapshot::Iterator::operator++() noexcept {
	++idx_;
	if (idx_ >= sn_->Size()) {
		idx_ = sn_->Size();
	}
	return *this;
}

Snapshot::Iterator& Snapshot::Iterator::operator+(size_t delta) noexcept {
	idx_ += delta;
	if (idx_ >= sn_->Size()) {
		idx_ = sn_->Size();
	}
	return *this;
}

}  // namespace reindexer
