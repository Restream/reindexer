#ifdef REINDEX_WITH_ROCKSDB

#include "rocksdbstorage.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include "tools/fsops.h"

namespace reindexer {
namespace datastorage {

using namespace std::string_view_literals;

constexpr auto kStorageNotInitialized = "Storage is not initialized"sv;
constexpr auto kLostDirName = "lost"sv;

static void toWriteOptions(const StorageOpts& opts, rocksdb::WriteOptions& wopts) noexcept { wopts.sync = opts.IsSync(); }

static void toReadOptions(const StorageOpts& opts, rocksdb::ReadOptions& ropts) noexcept {
	ropts.fill_cache = opts.IsFillCache();
	ropts.verify_checksums = opts.IsVerifyChecksums();
}

RocksDbStorage::RocksDbStorage() = default;

Error RocksDbStorage::Open(const std::string& path, const StorageOpts& opts) {
	if (path.empty()) {
		return Error(errParams, "Cannot enable storage: the path is empty");
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

void RocksDbStorage::Destroy(const std::string& path) {
	std::ignore = fs::RmDirAll(fs::JoinPath(path, std::string(kLostDirName)));

	rocksdb::Options options;
	options.create_if_missing = true;
	db_.reset();
	rocksdb::Status status = rocksdb::DestroyDB(path.c_str(), options);
	fprintf(stderr, "reindexer error: unable to remove RocksDB's storage: %s, %s. Trying to remove files using backup mechanism...\n",
			path.c_str(), status.ToString().c_str());
	if (fs::RmDirAll(path) != 0) {
		fprintf(stderr, "reindexer error: unable to remove RocksDB's storage: %s, %s", path.c_str(), strerror(errno));
	}
}

Error RocksDbStorage::Read(const StorageOpts& opts, std::string_view key, std::string& value) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}

	rocksdb::ReadOptions options;
	toReadOptions(opts, options);
	rocksdb::Status status = db_->Get(options, rocksdb::Slice(key.data(), key.size()), &value);
	if (status.ok()) {
		return Error();
	}
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error RocksDbStorage::Write(const StorageOpts& opts, std::string_view key, std::string_view value) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}

	rocksdb::WriteOptions options;
	toWriteOptions(opts, options);
	rocksdb::Status status = db_->Put(options, rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value.data(), value.size()));
	if (status.ok()) {
		return Error();
	}
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error RocksDbStorage::Write(const StorageOpts& opts, UpdatesCollection& buffer) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}

	rocksdb::WriteOptions options;
	toWriteOptions(opts, options);
	auto batchBuffer = static_cast<RocksDbBatchBuffer*>(&buffer);
	rocksdb::Status status = db_->Write(options, &batchBuffer->batchWrite_);
	if (status.ok()) {
		return Error();
	}
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error RocksDbStorage::Delete(const StorageOpts& opts, std::string_view key) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}

	rocksdb::WriteOptions options;
	toWriteOptions(opts, options);
	rocksdb::Status status = db_->Delete(options, rocksdb::Slice(key.data(), key.size()));
	if (status.ok()) {
		return Error();
	}
	return Error(errLogic, status.ToString());
}

Error RocksDbStorage::Repair(const std::string& path) {
	rocksdb::Options options;
	auto status = rocksdb::RepairDB(path, options);
	if (status.ok()) {
		return Error();
	}
	return Error(errLogic, status.ToString());
}

Snapshot::Ptr RocksDbStorage::MakeSnapshot() {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}
	const rocksdb::Snapshot* ldbSnapshot = db_->GetSnapshot();
	assertrx(ldbSnapshot);
	return std::make_shared<RocksDbSnapshot>(ldbSnapshot);
}

void RocksDbStorage::ReleaseSnapshot(Snapshot::Ptr snapshot) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}
	if (!snapshot) {
		throw Error(errParams, "Storage pointer is null");
	}
	const RocksDbSnapshot* rocksDbSnapshot = static_cast<const RocksDbSnapshot*>(snapshot.get());
	db_->ReleaseSnapshot(rocksDbSnapshot->snapshot_);
	snapshot.reset();
}

Error RocksDbStorage::Flush() {
	// RocksDB does not support Flush mechanism.
	// It just doesn't know when an asynchronous
	// write has completed. So the only way (the dump
	// way) is to just close the Storage and then open
	// it again. So that is what we do:
	if (db_) {
		db_.reset();
		return Open(dbpath_, opts_);
	}
	return Error();
}

Error RocksDbStorage::Reopen() {
	if (!dbpath_.empty()) {
		db_.reset();
		return Open(dbpath_, opts_);
	}
	return Error();
}

Cursor* RocksDbStorage::GetCursor(StorageOpts& opts) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}
	rocksdb::ReadOptions options;
	toReadOptions(opts, options);
	options.fill_cache = false;
	return new RocksDbIterator(db_->NewIterator(options));
}

UpdatesCollection* RocksDbStorage::GetUpdatesCollection() { return new RocksDbBatchBuffer(); }

std::string_view RocksDbIterator::Key() const {
	rocksdb::Slice key = iterator_->key();
	return std::string_view(key.data(), key.size());
}

std::string_view RocksDbIterator::Value() const {
	rocksdb::Slice key = iterator_->value();
	return std::string_view(key.data(), key.size());
}

int RocksDbComparator::Compare(std::string_view a, std::string_view b) const {
	rocksdb::Options options;
	return options.comparator->Compare(rocksdb::Slice(a.data(), a.size()), rocksdb::Slice(b.data(), b.size()));
}

RocksDbSnapshot::RocksDbSnapshot(const rocksdb::Snapshot* snapshot) noexcept : snapshot_(snapshot) {}

}  // namespace datastorage
}  // namespace reindexer
#else
// suppress clang warning
int ___rocksdbsrorage_dummy_suppress_warning;

#endif	// REINDEX_WITH_ROCKSDB
