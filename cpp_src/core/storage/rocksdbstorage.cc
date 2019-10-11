#ifdef REINDEX_WITH_ROCKSDB

#include "rocksdbstorage.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>

void toWriteOptions(const StorageOpts& opts, rocksdb::WriteOptions& wopts) { wopts.sync = opts.IsSync(); }

void toReadOptions(const StorageOpts& opts, rocksdb::ReadOptions& ropts) {
	ropts.fill_cache = opts.IsFillCache();
	ropts.verify_checksums = opts.IsVerifyChecksums();
}

namespace reindexer {
namespace datastorage {

constexpr auto kStorageNotInitialized = "Storage is not initialized"_sv;

RocksDbStorage::RocksDbStorage() {}

RocksDbStorage::~RocksDbStorage() {}

Error RocksDbStorage::Read(const StorageOpts& opts, const string_view& key, string& value) {
	if (!db_) throw Error(errParams, kStorageNotInitialized);

	rocksdb::ReadOptions options;
	toReadOptions(opts, options);
	rocksdb::Status status = db_->Get(options, rocksdb::Slice(key.data(), key.size()), &value);
	if (status.ok()) return Error();
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error RocksDbStorage::Write(const StorageOpts& opts, const string_view& key, const string_view& value) {
	if (!db_) throw Error(errParams, kStorageNotInitialized);

	rocksdb::WriteOptions options;
	toWriteOptions(opts, options);
	rocksdb::Status status = db_->Put(options, rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value.data(), value.size()));
	if (status.ok()) return Error();
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error RocksDbStorage::Write(const StorageOpts& opts, UpdatesCollection& buffer) {
	if (!db_) throw Error(errParams, kStorageNotInitialized);

	rocksdb::WriteOptions options;
	toWriteOptions(opts, options);
	RocksDbBatchBuffer* batchBuffer = static_cast<RocksDbBatchBuffer*>(&buffer);
	rocksdb::Status status = db_->Write(options, &batchBuffer->batchWrite_);
	if (status.ok()) return Error();
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error RocksDbStorage::Delete(const StorageOpts& opts, const string_view& key) {
	if (!db_) throw Error(errParams, kStorageNotInitialized);

	rocksdb::WriteOptions options;
	toWriteOptions(opts, options);
	rocksdb::Status status = db_->Delete(options, rocksdb::Slice(key.data(), key.size()));
	if (status.ok()) return Error();
	return Error(errLogic, status.ToString());
}

Error RocksDbStorage::Repair(const std::string& path) {
	rocksdb::Options options;
	auto status = rocksdb::RepairDB(path, options);
	if (status.ok()) return Error();
	return Error(errLogic, status.ToString());
}

Snapshot::Ptr RocksDbStorage::MakeSnapshot() {
	if (!db_) throw Error(errParams, kStorageNotInitialized);
	const rocksdb::Snapshot* ldbSnapshot = db_->GetSnapshot();
	assert(ldbSnapshot);
	return std::make_shared<RocksDbSnapshot>(ldbSnapshot);
}

void RocksDbStorage::ReleaseSnapshot(Snapshot::Ptr snapshot) {
	if (!db_) throw Error(errParams, kStorageNotInitialized);
	if (!snapshot) throw Error(errParams, "Storage pointer is null");
	const RocksDbSnapshot* levelDbSnpshot = static_cast<const RocksDbSnapshot*>(snapshot.get());
	db_->ReleaseSnapshot(levelDbSnpshot->snapshot_);
	snapshot.reset();
}

void RocksDbStorage::Flush() {
	// RocksDB does not support Flush mechanism.
	// It just doesn't know when an asynchronous
	// write has completed. So the only way (the dump
	// way) is to just close the Storage and then open
	// it again. So that is what we do:
	db_.reset();
	Open(dbpath_, opts_);
}

Cursor* RocksDbStorage::GetCursor(StorageOpts& opts) {
	if (!db_) throw Error(errParams, kStorageNotInitialized);
	rocksdb::ReadOptions options;
	toReadOptions(opts, options);
	options.fill_cache = false;
	return new RocksDbIterator(db_->NewIterator(options));
}

UpdatesCollection* RocksDbStorage::GetUpdatesCollection() { return new RocksDbBatchBuffer(); }

Error RocksDbStorage::doOpen(const string& path, const StorageOpts& opts) {
	if (path.empty()) {
		throw Error(errParams, "Cannot enable storage: the path is empty '%s'", path);
	}

	rocksdb::Options options;
	options.create_if_missing = opts.IsCreateIfMissing();
	options.max_open_files = 50;

	rocksdb::DB* db;
	rocksdb::Status status = rocksdb::DB::Open(options, path, &db);
	if (status.ok()) {
		db_.reset(db);
		opts_ = opts;
		dbpath_ = path;
		return Error();
	}

	return Error(errLogic, status.ToString());
}

void RocksDbStorage::doDestroy(const string& path) {
	rocksdb::Options options;
	options.create_if_missing = true;
	db_.reset();
	rocksdb::Status status = rocksdb::DestroyDB(path.c_str(), options);
	if (!status.ok()) {
		printf("Cannot destroy DB: %s, %s\n", path.c_str(), status.ToString().c_str());
	}
}

RocksDbBatchBuffer::RocksDbBatchBuffer() {}

RocksDbBatchBuffer::~RocksDbBatchBuffer() {}

void RocksDbBatchBuffer::Put(const string_view& key, const string_view& value) {
	batchWrite_.Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value.data(), value.size()));
}

void RocksDbBatchBuffer::Remove(const string_view& key) { batchWrite_.Delete(rocksdb::Slice(key.data(), key.size())); }

void RocksDbBatchBuffer::Clear() { batchWrite_.Clear(); }

RocksDbIterator::RocksDbIterator(rocksdb::Iterator* iterator) : iterator_(iterator) {}

RocksDbIterator::~RocksDbIterator() {}

bool RocksDbIterator::Valid() const { return iterator_->Valid(); }

void RocksDbIterator::SeekToFirst() { return iterator_->SeekToFirst(); }

void RocksDbIterator::SeekToLast() { return iterator_->SeekToLast(); }

void RocksDbIterator::Seek(const string_view& target) { return iterator_->Seek(rocksdb::Slice(target.data(), target.size())); }

void RocksDbIterator::Next() { return iterator_->Next(); }

void RocksDbIterator::Prev() { return iterator_->Prev(); }

string_view RocksDbIterator::Key() const {
	rocksdb::Slice key = iterator_->key();
	return string_view(key.data(), key.size());
}

string_view RocksDbIterator::Value() const {
	rocksdb::Slice key = iterator_->value();
	return string_view(key.data(), key.size());
}

Comparator& RocksDbIterator::GetComparator() { return comparator_; }

int RocksDbComparator::Compare(const string_view& a, const string_view& b) const {
	rocksdb::Options options;
	return options.comparator->Compare(rocksdb::Slice(a.data(), a.size()), rocksdb::Slice(b.data(), b.size()));
}

RocksDbSnapshot::RocksDbSnapshot(const rocksdb::Snapshot* snapshot) : snapshot_(snapshot) {}

RocksDbSnapshot::~RocksDbSnapshot() { snapshot_ = nullptr; }
}  // namespace datastorage
}  // namespace reindexer
#else
// suppress clang warngig
int ___rocksdbsrorage_dummy_suppress_warning;

#endif  // REINDEX_WITH_ROCKSDB
