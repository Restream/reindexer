
#include "waltracker.h"
#include "tools/logger.h"
#include "tools/serializer.h"

#define kStorageWALPrefix "W"

namespace reindexer {

WALTracker::WALTracker(int64_t sz) : walSize_(sz) { logPrintf(LogTrace, "[WALTracker] Create LSN=%ld", lsnCounter_); }

lsn_t WALTracker::Add(const WALRecord &rec, lsn_t originLsn, lsn_t oldLsn) {
	return add(rec, originLsn, rec.type != WalItemUpdate, oldLsn);
}

lsn_t WALTracker::Add(WALRecType type, const PackedWALRecord &rec, lsn_t originLsn) {
	return add(rec, originLsn, type != WalItemUpdate && type != WalEmpty);
}  // TODO: Check storage write condition

bool WALTracker::Set(const WALRecord &rec, lsn_t lsn, bool ignoreServer) {
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
	// logPrintf(LogWarning, "WALRecord for LSN = %d is not empty", lsn);
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
		Set(WALRecord(span<uint8_t>(oldRecords[pos])), lsn_t(lsn, oldRecords[pos].server), true);
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
	auto storage = storage_.lock();
	if (storage) {
		StorageOpts opts;
		std::unique_ptr<datastorage::Cursor> dbIter(storage->GetCursor(opts));
		for (dbIter->Seek(kStorageWALPrefix);
			 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kStorageWALPrefix "\xFF")) < 0;
			 dbIter->Next()) {
			storage->Delete(opts, dbIter->Key());
		}
	}
}

void WALTracker::SetStorage(shared_ptr<datastorage::IDataStorage> storage) {
	assert(!storage_.lock());
	storage_ = storage;
}

void WALTracker::Init(int64_t sz, int64_t minLSN, int64_t maxLSN, shared_ptr<datastorage::IDataStorage> storage) {
	logPrintf(LogTrace, "WALTracker::Init minLSN=%ld, maxLSN=%ld, size=%ld", minLSN, maxLSN, sz);
	storage_ = storage;

	// input maxLSN of namespace Item or -1 if namespace is empty
	auto data = readFromStorage(maxLSN);  // return maxLSN of wal record or input value
										  // new table
	initPositions(sz, minLSN, maxLSN);
	// Fill records from storage
	for (auto &rec : data) {
		Set(WALRecord(std::string_view(rec.second)), rec.first, true);
	}
}

void WALTracker::put(lsn_t lsn, const WALRecord &rec) {
	int64_t pos = lsn.Counter() % walSize_;
	if (pos >= int64_t(records_.size())) {
		records_.resize(uint64_t(pos + 1));
	}

	heapSize_ -= records_[pos].heap_size();
	records_[pos].Pack(lsn.Server(), rec);
	heapSize_ += records_[pos].heap_size();
}

void WALTracker::put(lsn_t lsn, const PackedWALRecord &rec) {
	int64_t pos = lsn.Counter() % walSize_;
	assert(pos >= 0 && size_t(pos) <= records_.size());
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
		if (records_[pos].server == lsnUnp.server || ignoreServer) {
			return true;  // Record matches
		}
		return records_[pos].server == 0 &&
			   WALRecord(records_[pos]).type == WalEmpty;  // true if empty record was loaded from storage without server ID
	}
	return false;
}

void WALTracker::writeToStorage(lsn_t lsn) {
	auto storage = storage_.lock();
	if (storage) {
		int64_t pos = lsn.Counter() % walSize_;

		WrSerializer key, data;
		key << kStorageWALPrefix;
		key.PutUInt32(uint32_t(pos));
		data.PutUInt64(int64_t(lsn));
		data.Write(std::string_view(reinterpret_cast<char *>(records_[pos].data()), records_[pos].size()));

		storage->Write(StorageOpts(), key.Slice(), data.Slice());
	}
}

std::vector<std::pair<lsn_t, std::string>> WALTracker::readFromStorage(int64_t &maxLSN) {
	std::vector<std::pair<lsn_t, std::string>> data;

	StorageOpts opts;
	opts.FillCache(false);

	auto storage = storage_.lock();
	if (!storage) return data;

	std::unique_ptr<datastorage::Cursor> dbIter(storage->GetCursor(opts));

	for (dbIter->Seek(kStorageWALPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kStorageWALPrefix "\xFF")) < 0;
		 dbIter->Next()) {
		std::string_view dataSlice = dbIter->Value();
		if (dataSlice.size() >= sizeof(int64_t)) {
			// Read LSN
			lsn_t lsn(*reinterpret_cast<const int64_t *>(dataSlice.data()));
			assert(lsn.Counter() >= 0);
			maxLSN = std::max(maxLSN, lsn.Counter());
			dataSlice = dataSlice.substr(sizeof(int64_t));
			data.push_back({lsn_t(lsn), string(dataSlice)});
		}
	}

	return data;
}

void WALTracker::initPositions(int64_t sz, int64_t minLSN, int64_t maxLSN) {
	int64_t counter = maxLSN + 1;
	lsnCounter_.SetCounter(counter);
	lastLsn_ = lsn_t(maxLSN, lsnCounter_.Server());
	walSize_ = sz;
	records_.clear();
	records_.resize(std::min(counter, walSize_));
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
lsn_t WALTracker::add(RecordT &&rec, lsn_t originLsn, bool toStorage, lsn_t oldLsn) {
	lsn_t lsn = originLsn;
	if (lsn.isEmpty()) {
		lsn = lsnCounter_++;
	} else if (lsnCounter_.Counter() > originLsn.Counter()) {
		throw Error(errLogic, "Unexpected origin LSN count: %d. Expecting at least %d", int64_t(lsn.Counter()),
					int64_t(lsnCounter_.Counter()));
	} else {
		auto newCounter = lsn.Counter();
		if (lsnCounter_.Counter() == 0) {  // If there are no WAL records and we've got record with some large LSN
			assert(records_.empty());
			records_.resize(newCounter % walSize_);
		}
		lsnCounter_.SetCounter(newCounter + 1);
	}
	lastLsn_ = lsn;
	const auto counter = lsnCounter_.Counter();
	if (counter > 1 && walOffset_ == (counter - 1) % walSize_) {
		walOffset_ = counter % walSize_;
	}

	put(lastLsn_, std::forward<RecordT>(rec));
	if (!oldLsn.isEmpty() && available(oldLsn)) {
		put(oldLsn, WALRecord());
	}
	if (toStorage) {
		writeToStorage(lastLsn_);
	}
	return lastLsn_;
}

}  // namespace reindexer
