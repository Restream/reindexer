
#include "waltracker.h"
#include "tools/serializer.h"

#define kStorageWALPrefix "W"

namespace reindexer {

int64_t WALTracker::Add(const WALRecord &rec, int64_t oldLsn) {
	int64_t lsn = lsnCounter_++;
	put(lsn, rec);
	if (oldLsn >= 0 && available(oldLsn)) {
		put(oldLsn, WALRecord());
	}

	if (rec.type != WalItemUpdate) {
		//
		writeToStorage(lsn);
	}

	return lsn;
}

bool WALTracker::Set(const WALRecord &rec, int64_t lsn) {
	if (!available(lsn)) {
		return false;
	}
	put(lsn, rec);
	return true;
}

void WALTracker::Init(int64_t maxLSN, shared_ptr<datastorage::IDataStorage> storage) {
	storage_ = storage;
	auto data = readFromStorage(maxLSN);
	maxLSN++;

	records_.clear();
	records_.resize(std::min(maxLSN, walSize_));
	lsnCounter_ = maxLSN;

	// Fill records from storage
	for (auto &rec : data) {
		Set(WALRecord(string_view(rec.second)), rec.first);
	}
}

void WALTracker::put(int64_t lsn, const WALRecord &rec) {
	uint64_t pos = lsn % walSize_;
	if (pos >= records_.size()) records_.resize(pos + 1);
	records_[pos].Pack(rec);
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

size_t WALTracker::heap_size() const {
	size_t ret = records_.capacity() * sizeof(PackedWALRecord);
	for (auto &rec : records_) ret += rec.heap_size();

	return ret;
}

}  // namespace reindexer
