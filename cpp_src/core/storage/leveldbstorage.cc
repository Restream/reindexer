#include "leveldbstorage.h"

#include <leveldb/comparator.h>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/slice.h>

const char* storageNotInitialized = "Storage is not initialized";

void toWriteOptions(const StorageOpts& opts, leveldb::WriteOptions& wopts) { wopts.sync = opts.IsSync(); }

void toReadOptions(const StorageOpts& opts, leveldb::ReadOptions& ropts) {
	ropts.fill_cache = opts.IsFillCache();
	ropts.verify_checksums = opts.IsVerifyChecksums();
}

namespace reindexer {
namespace datastorage {

LevelDbStorage::LevelDbStorage() {}

LevelDbStorage::~LevelDbStorage() {}

Error LevelDbStorage::Open(const string& path, const StorageOpts& opts) {
	if (path.empty()) {
		throw Error(errParams, "Cannot enable storage: the path is empty '%s'", path.c_str());
	}

	leveldb::Options options;
	options.create_if_missing = opts.IsCreateIfMissing();
	options.max_open_files = 50;

	leveldb::DB* db;
	leveldb::Status status = leveldb::DB::Open(options, path.c_str(), &db);
	if (status.ok()) {
		db_ = shared_ptr<leveldb::DB>(db);
	}

	if (status.ok()) {
		opts_ = opts;
		dbpath_ = path;
		return Error();
	}

	return Error(errLogic, "%s", status.ToString().c_str());
}

Error LevelDbStorage::Read(const StorageOpts& opts, const string_view& key, string& value) {
	if (!db_) throw Error(errParams, "%s", storageNotInitialized);

	leveldb::ReadOptions options;
	toReadOptions(opts, options);
	leveldb::Status status = db_->Get(options, leveldb::Slice(key.data(), key.size()), &value);
	if (status.ok()) return Error();
	return Error(status.IsNotFound() ? errNotFound : errLogic, "%s", status.ToString().c_str());
}

Error LevelDbStorage::Write(const StorageOpts& opts, const string_view& key, const string_view& value) {
	if (!db_) throw Error(errParams, "%s", storageNotInitialized);

	leveldb::WriteOptions options;
	toWriteOptions(opts, options);
	leveldb::Status status = db_->Put(options, leveldb::Slice(key.data(), key.size()), leveldb::Slice(value.data(), value.size()));
	if (status.ok()) return Error();
	return Error(status.IsNotFound() ? errNotFound : errLogic, "%s", status.ToString().c_str());
}

Error LevelDbStorage::Write(const StorageOpts& opts, UpdatesCollection& buffer) {
	if (!db_) throw Error(errParams, "%s", storageNotInitialized);

	leveldb::WriteOptions options;
	toWriteOptions(opts, options);
	LevelDbBatchBuffer* batchBuffer = static_cast<LevelDbBatchBuffer*>(&buffer);
	leveldb::Status status = db_->Write(options, &batchBuffer->batchWrite_);
	if (status.ok()) return Error();
	return Error(status.IsNotFound() ? errNotFound : errLogic, "%s", status.ToString().c_str());
}

Error LevelDbStorage::Delete(const StorageOpts& opts, const string_view& key) {
	if (!db_) throw Error(errParams, "%s", storageNotInitialized);

	leveldb::WriteOptions options;
	toWriteOptions(opts, options);
	leveldb::Status status = db_->Delete(options, leveldb::Slice(key.data(), key.size()));
	if (status.ok()) return Error();
	return Error(errLogic, "%s", status.ToString().c_str());
}

Snapshot::Ptr LevelDbStorage::MakeSnapshot() {
	if (!db_) throw Error(errParams, "%s", storageNotInitialized);
	const leveldb::Snapshot* ldbSnapshot = db_->GetSnapshot();
	assert(ldbSnapshot);
	return std::make_shared<LevelDbSnapshot>(ldbSnapshot);
}

void LevelDbStorage::ReleaseSnapshot(Snapshot::Ptr snapshot) {
	if (!db_) throw Error(errParams, "%s", storageNotInitialized);
	if (!snapshot) throw Error(errParams, "Storage pointer is null");
	const LevelDbSnapshot* levelDbSnpshot = static_cast<const LevelDbSnapshot*>(snapshot.get());
	db_->ReleaseSnapshot(levelDbSnpshot->snapshot_);
	snapshot.reset();
}

void LevelDbStorage::Flush() {
	// LevelDB does not support Flush mechanism.
	// It just doesn't know when an asynchronous
	// write has completed. So the only way (the dump
	// way) is to just close the Storage and then open
	// it again. So that is what we do:
	db_.reset();
	Open(dbpath_, opts_);
}

void LevelDbStorage::Destroy(const string& path) {
	leveldb::Options options;
	options.create_if_missing = true;
	db_.reset();
	leveldb::Status status = leveldb::DestroyDB(path.c_str(), options);
	if (!status.ok()) {
		printf("Cannot destroy DB: %s, %s\n", path.c_str(), status.ToString().c_str());
	}
}

Cursor* LevelDbStorage::GetCursor(StorageOpts& opts) {
	if (!db_) throw Error(errParams, "%s", storageNotInitialized);
	leveldb::ReadOptions options;
	toReadOptions(opts, options);
	options.fill_cache = false;
	return new LevelDbIterator(db_->NewIterator(options));
}

UpdatesCollection* LevelDbStorage::GetUpdatesCollection() { return new LevelDbBatchBuffer(); }

LevelDbBatchBuffer::LevelDbBatchBuffer() {}

LevelDbBatchBuffer::~LevelDbBatchBuffer() {}

void LevelDbBatchBuffer::Put(const string_view& key, const string_view& value) {
	batchWrite_.Put(leveldb::Slice(key.data(), key.size()), leveldb::Slice(value.data(), value.size()));
}

void LevelDbBatchBuffer::Remove(const string_view& key) { batchWrite_.Delete(leveldb::Slice(key.data(), key.size())); }

void LevelDbBatchBuffer::Clear() { batchWrite_.Clear(); }

LevelDbIterator::LevelDbIterator(leveldb::Iterator* iterator) : iterator_(iterator) {}

LevelDbIterator::~LevelDbIterator() {}

bool LevelDbIterator::Valid() const { return iterator_->Valid(); }

void LevelDbIterator::SeekToFirst() { return iterator_->SeekToFirst(); }

void LevelDbIterator::SeekToLast() { return iterator_->SeekToLast(); }

void LevelDbIterator::Seek(const string_view& target) { return iterator_->Seek(leveldb::Slice(target.data(), target.size())); }

void LevelDbIterator::Next() { return iterator_->Next(); }

void LevelDbIterator::Prev() { return iterator_->Prev(); }

string_view LevelDbIterator::Key() const {
	leveldb::Slice key = iterator_->key();
	return string_view(key.data(), key.size());
}

string_view LevelDbIterator::Value() const {
	leveldb::Slice key = iterator_->value();
	return string_view(key.data(), key.size());
}

Comparator& LevelDbIterator::GetComparator() { return comparator_; }

int LevelDbComparator::Compare(const string_view& a, const string_view& b) const {
	leveldb::Options options;
	return options.comparator->Compare(leveldb::Slice(a.data(), a.size()), leveldb::Slice(b.data(), b.size()));
}

LevelDbSnapshot::LevelDbSnapshot(const leveldb::Snapshot* snapshot) : snapshot_(snapshot) {}

LevelDbSnapshot::~LevelDbSnapshot() { snapshot_ = nullptr; }
}  // namespace datastorage
}  // namespace reindexer
