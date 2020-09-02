
#include "waltracker.h"
#include "tools/logger.h"
#include "tools/serializer.h"

#define kStorageWALPrefix "W"

namespace reindexer {

WALTracker::WALTracker(int64_t sz) : walSize_(sz) { logPrintf(LogTrace, "[WALTracker] Create LSN=%ld", lsnCounter_); }

int64_t WALTracker::Add(const WALRecord &rec, lsn_t oldLsn) {
	int64_t lsn = lsnCounter_++;
	if (lsnCounter_ > 1 && walOffset_ == (lsnCounter_ - 1) % walSize_) {
		walOffset_ = lsnCounter_ % walSize_;
	}

	put(lsn, rec);
	if (!oldLsn.isEmpty() && available(oldLsn.Counter())) {
		put(oldLsn.Counter(), WALRecord());
	}
	if (rec.type != WalItemUpdate) {
		writeToStorage(lsn);
	}
	return lsn;
}

bool WALTracker::Set(const WALRecord &rec, int64_t lsn) {
	if (!available(lsn)) {
		return false;
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

bool WALTracker::Resize(int64_t sz) {
	auto oldSz = Capacity();
	if (sz == oldSz) {
		return false;
	}

	int64_t minLSN = std::numeric_limits<int64_t>::max();
	int64_t maxLSN = -1;
	auto filledSize = size();
	if (filledSize) {
		maxLSN = lsnCounter_ - 1;
		minLSN = maxLSN - ((sz > filledSize ? filledSize : sz) - 1);
	}

	std::vector<PackedWALRecord> oldRecords;
	std::swap(records_, oldRecords);
	initPositions(sz, minLSN, maxLSN);
	for (auto lsn = minLSN; lsn <= maxLSN; ++lsn) {
		Set(WALRecord(span<uint8_t>(oldRecords[lsn % oldSz])), lsn);
	}
	return true;
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
		Set(WALRecord(string_view(rec.second)), rec.first);
	}
}

void WALTracker::put(int64_t lsn, const WALRecord &rec) {
	int64_t pos = lsn % walSize_;
	if (pos >= int64_t(records_.size())) {
		records_.resize(uint64_t(pos + 1));
	}

	heapSize_ -= records_[pos].heap_size();
	records_[pos].Pack(rec);
	heapSize_ += records_[pos].heap_size();
}

void WALTracker::writeToStorage(int64_t lsn) {
	uint64_t pos = lsn % walSize_;

	WrSerializer key, data;
	key << kStorageWALPrefix;
	key.PutUInt32(pos);
	data.PutUInt64(lsn);
	data.Write(string_view(reinterpret_cast<char *>(records_[pos].data()), records_[pos].size()));
	auto storage = storage_.lock();
	if (storage) storage->Write(StorageOpts(), key.Slice(), data.Slice());
}

std::vector<std::pair<int64_t, std::string>> WALTracker::readFromStorage(int64_t &maxLSN) {
	std::vector<std::pair<int64_t, std::string>> data;

	StorageOpts opts;
	opts.FillCache(false);

	auto storage = storage_.lock();
	if (!storage) return data;

	std::unique_ptr<datastorage::Cursor> dbIter(storage->GetCursor(opts));

	for (dbIter->Seek(kStorageWALPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), string_view(kStorageWALPrefix "\xFF")) < 0; dbIter->Next()) {
		string_view dataSlice = dbIter->Value();
		if (dataSlice.size() >= sizeof(int64_t)) {
			// Read LSN
			int64_t lsn = *reinterpret_cast<const int64_t *>(dataSlice.data());
			assert(lsn >= 0);
			maxLSN = std::max(maxLSN, lsn);
			dataSlice = dataSlice.substr(sizeof(lsn));
			data.push_back({lsn, string(dataSlice)});
		}
	}

	return data;
}

void WALTracker::initPositions(int64_t sz, int64_t minLSN, int64_t maxLSN) {
	lsnCounter_ = maxLSN + 1;
	walSize_ = sz;
	records_.clear();
	records_.resize(std::min(lsnCounter_, walSize_));
	heapSize_ = 0;
	if (minLSN == std::numeric_limits<int64_t>::max()) {
		walOffset_ = 0;
	} else {
		if (lsnCounter_ > walSize_ + minLSN) {
			walOffset_ = lsnCounter_ % walSize_;
		} else {
			walOffset_ = minLSN % walSize_;
		}
	}
}

}  // namespace reindexer
