
#include "waltracker.h"
#include "core/namespace/asyncstorage.h"
#include "tools/logger.h"
#include "tools/serializer.h"

#define kStorageWALPrefix "W"

namespace reindexer {

WALTracker::WALTracker(int64_t sz) : walSize_(sz) { logFmt(LogTrace, "[WALTracker] Create LSN={}", lsnCounter_); }

WALTracker::WALTracker(const WALTracker& wal, AsyncStorage& storage)
	: records_(wal.records_),
	  lsnCounter_(wal.lsnCounter_),
	  lastLsn_(wal.lastLsn_),
	  walSize_(wal.walSize_),
	  walOffset_(wal.walOffset_),
	  heapSize_(wal.heapSize_),
	  storage_(&storage) {}

lsn_t WALTracker::Add(const WALRecord& rec, lsn_t originLsn, lsn_t oldLsn) {
	return add(rec, originLsn, rec.type != WalItemUpdate, oldLsn);
}

lsn_t WALTracker::Add(WALRecType type, const PackedWALRecord& rec, lsn_t originLsn) { return add(rec, originLsn, type != WalItemUpdate); }

bool WALTracker::Set(const WALRecord& rec, lsn_t lsn, bool ignoreServer) {
	if (!available(lsn, ignoreServer)) {
		return false;
	}
	if (lsn.Counter() == lastLsn_.Counter()) {
		lastLsn_ = lsn;
	}
	// size_t pos = lsn % walSize_;
	// if ((pos >= records_.size()) || records_[pos].empty()) {
	put(lsn, rec);
	return true;
	// } else {
	// logFmt(LogWarning, "WALRecord for LSN = {} is not empty", lsn);
	// }
	// return false;
}

lsn_t WALTracker::FirstLSN() const noexcept {
	const auto counter = lsnCounter_.Counter();
	if (counter == 0) {
		return lsn_t();
	}
	return lsn_t(counter - size(), records_[walOffset_].server);
}

lsn_t WALTracker::LSNByOffset(int64_t offset) const noexcept {
	if (offset < 0 || offset >= size()) {
		return FirstLSN();
	}
	const auto counter = lsnCounter_.Counter();
	if (counter == 0) {
		return lsn_t();
	}
	return lsn_t(counter - offset, records_[(counter - offset) % walSize_].server);
}

bool WALTracker::Resize(int64_t sz) {
	const auto oldSz = Capacity();
	if (sz == oldSz) {
		return false;
	}

	int64_t minLSN = std::numeric_limits<int64_t>::max();
	int64_t maxLSN = -1;
	auto filledSize = size();
	if (filledSize) {
		maxLSN = lsnCounter_.Counter() - 1;
		minLSN = maxLSN - ((sz > filledSize ? filledSize : sz) - 1);
	}

	std::vector<MarkedPackedWALRecord> oldRecords;
	std::swap(records_, oldRecords);
	initPositions(sz, minLSN, maxLSN);
	for (auto lsn = minLSN; lsn <= maxLSN; ++lsn) {
		auto pos = lsn % oldSz;
		std::ignore = Set(WALRecord(std::span<uint8_t>(oldRecords[pos])), lsn_t(lsn, oldRecords[pos].server), true);
	}
	return true;
}

void WALTracker::Reset() {
	records_.clear();
	lsnCounter_ = lsn_t(0, lsnCounter_.Server());
	walOffset_ = 0;
	lastLsn_ = lsn_t();
	std::vector<MarkedPackedWALRecord> oldRecords;
	std::swap(records_, oldRecords);
	heapSize_ = 0;
	if (storage_ && storage_->IsValid()) {
		StorageOpts opts;
		auto dbIter = storage_->GetCursor(opts);
		for (dbIter->Seek(kStorageWALPrefix);
			 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kStorageWALPrefix "\xFF\xFF\xFF\xFF")) < 0;
			 dbIter->Next()) {
			dbIter.RemoveThisKey(opts);
		}
	}
}

void WALTracker::Init(int64_t sz, int64_t minLSN, int64_t maxLSN, AsyncStorage& storage) {
	storage_ = &storage;

	// input maxLSN of namespace Item or -1 if namespace is empty
	auto data = readFromStorage(maxLSN);  // return maxLSN of wal record or input value
										  // new table
	logFmt(LogTrace, "WALTracker::Init minLSN={}, maxLSN={}, size={}", minLSN, maxLSN, sz);
	initPositions(sz, minLSN, maxLSN);
	// Fill records from storage
	for (auto& rec : data) {
		std::ignore = Set(WALRecord(std::string_view(rec.second)), rec.first, true);
	}
}

void WALTracker::put(lsn_t lsn, const WALRecord& rec) {
	int64_t pos = lsn.Counter() % walSize_;
	if (pos >= int64_t(records_.size())) {
		records_.resize(uint64_t(pos + 1), GetServer());
	}

	heapSize_ -= records_[pos].heap_size();
	records_[pos].Pack(lsn.Server(), rec);
	heapSize_ += records_[pos].heap_size();
}

void WALTracker::put(lsn_t lsn, const PackedWALRecord& rec) {
	int64_t pos = lsn.Counter() % walSize_;
	assertrx(pos >= 0 && size_t(pos) <= records_.size());
	if (pos == int64_t(records_.size())) {
		records_.emplace_back(lsn.Server(), rec);  // TODO: count heap size for the vector itself
		heapSize_ += records_[pos].heap_size();
		return;
	}

	heapSize_ -= records_[pos].heap_size();
	records_[pos] = MarkedPackedWALRecord(lsn.Server(), rec);
	heapSize_ += records_[pos].heap_size();
}

bool WALTracker::available(lsn_t lsn, bool ignoreServer) const {
	auto lsnUnp = lsn.Unpack();
	auto counter = lsnCounter_.Counter();
	if (lsnUnp.counter < counter && counter - lsnUnp.counter <= size()) {
		int64_t pos = lsnUnp.counter % walSize_;
		return (records_[pos].server == lsnUnp.server || ignoreServer);
	}
	return false;
}

void WALTracker::writeToStorage(lsn_t lsn) {
	if (storage_ && storage_->IsValid()) {
		int64_t pos = lsn.Counter() % walSize_;

		WrSerializer key, data;
		key << kStorageWALPrefix;
		key.PutUInt32(uint32_t(pos));
		data.PutUInt64(int64_t(lsn));
		data.Write(std::string_view(reinterpret_cast<char*>(records_[pos].data()), records_[pos].size()));

		storage_->WriteSync(StorageOpts(), key.Slice(), data.Slice());
	}
}

std::vector<std::pair<lsn_t, std::string>> WALTracker::readFromStorage(int64_t& maxLSN) {
	std::vector<std::pair<lsn_t, std::string>> data;

	StorageOpts opts;
	opts.FillCache(false);

	if (!storage_ || !storage_->IsValid()) {
		return data;
	}

	auto dbIter = storage_->GetCursor(opts);
	for (dbIter->Seek(kStorageWALPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kStorageWALPrefix "\xFF\xFF\xFF\xFF")) < 0;
		 dbIter->Next()) {
		std::string_view dataSlice = dbIter->Value();
		if (dataSlice.size() >= sizeof(int64_t)) {
			// Read LSN
			Serializer ser(dataSlice);
			lsn_t lsn(int64_t(ser.GetUInt64()));
			assertrx(lsn.Counter() >= 0);
			maxLSN = std::max(maxLSN, lsn.Counter());
			dataSlice = dataSlice.substr(ser.Pos());
			data.push_back({lsn_t(lsn), std::string(dataSlice)});
		}
	}

	return data;
}

void WALTracker::initPositions(int64_t sz, int64_t minLSN, int64_t maxLSN) {
	int64_t counter = maxLSN + 1;
	lsnCounter_.SetCounter(counter);
	lastLsn_ = lsn_t(maxLSN, GetServer());
	walSize_ = sz;
	records_.clear();
	records_.resize(std::min(counter, walSize_), GetServer());
	heapSize_ = 0;
	if (minLSN == std::numeric_limits<int64_t>::max() || !walSize_) {
		walOffset_ = 0;
	} else {
		if (counter > walSize_ + minLSN) {
			walOffset_ = counter % walSize_;
		} else {
			walOffset_ = minLSN % walSize_;
		}
	}
}

template <typename RecordT>
lsn_t WALTracker::add(RecordT&& rec, lsn_t originLsn, bool toStorage, lsn_t oldLsn) {
	const auto localServerID = GetServer();
	const auto initialLsnCounter = lsnCounter_.Counter();
	const bool emptyWAL = (initialLsnCounter == 0);
	lsn_t lsn = originLsn;
	if (lsn.isEmpty()) {
		lsn = lsnCounter_++;
	} else if (initialLsnCounter > lsn.Counter()) {
		throw Error(errLogic, "Unexpected origin LSN count: {}. Expecting at least {}", int64_t(lsn.Counter()), int64_t(initialLsnCounter));
	} else {
		auto newCounter = lsn.Counter();
		if (emptyWAL && walSize_) {	 // If there are no WAL records and we've got record with some large LSN
			assertrx(records_.empty());
			assertrx(!walOffset_);
			walOffset_ = newCounter % walSize_;
			logFmt(LogInfo, "[wal:{}] Setting wal offset to {}; LSN {}", GetServer(), walOffset_, originLsn);
			records_.resize(walOffset_, localServerID);
		}
		lsnCounter_.SetCounter(newCounter + 1);
	}
	lastLsn_ = lsn;
	if (!walSize_) {
		return lastLsn_;
	}
	const auto counter = lsnCounter_.Counter();
	if (counter > 1 && walOffset_ == (counter - 1) % walSize_ && !emptyWAL) {
		walOffset_ = counter % walSize_;
	}

	put(lastLsn_, std::forward<RecordT>(rec));
	if (!oldLsn.isEmpty() && available(oldLsn)) {
		put(oldLsn, WALRecord());
		if (oldLsn.Server() != localServerID) {
			writeToStorage(oldLsn);	 // Write empty record to the storage to preserve it's server ID
		}
	}
	if (toStorage) {
		writeToStorage(lastLsn_);
	}
	return lastLsn_;
}

}  // namespace reindexer
