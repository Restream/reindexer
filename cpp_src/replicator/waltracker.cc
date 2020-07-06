
#include "waltracker.h"
#include "tools/logger.h"
#include "tools/serializer.h"

#define kStorageWALPrefix "W"

namespace reindexer {

WALTracker::WALTracker() { logPrintf(LogTrace, "[WALTracker] Create LSN=%ld", lsnCounter_); }

int64_t WALTracker::Add(const WALRecord &rec, lsn_t oldLsn) {
	int64_t lsn = lsnCounter_++;
	put(lsn, rec);
	if (!oldLsn.isEmpty()) {
		if (available(oldLsn.Counter())) {
			put(oldLsn.Counter(), WALRecord());
		}
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

void WALTracker::Init(int64_t maxLSN, shared_ptr<datastorage::IDataStorage> storage) {
	logPrintf(LogTrace, "WALTracker::Init maxLSN=%ld", maxLSN);
	storage_ = storage;
	// input maxLSN of namespace Item or -1 if namespace is empty
	auto data = readFromStorage(maxLSN);  // return maxLSN of wal record or input value
	if (maxLSN == -1)					  // new table
	{
		maxLSN = lsnCounter_;
	} else {
		maxLSN++;
		lsnCounter_ = maxLSN;
	}
	records_.clear();
	records_.resize(std::min(maxLSN, walSize_));
	heapSize_ = 0;
	// Fill records from storage
	for (auto &rec : data) {
		Set(WALRecord(string_view(rec.second)), rec.first);
	}
}

void WALTracker::put(int64_t lsn, const WALRecord &rec) {
	uint64_t pos = lsn % walSize_;
	if (pos >= records_.size()) records_.resize(pos + 1);
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

}  // namespace reindexer
