#ifdef REINDEX_WITH_LEVELDB

#include "leveldbstorage.h"

#include <leveldb/comparator.h>
#include <leveldb/db.h>
#include <leveldb/slice.h>
#include "leveldblogger.h"
#include "tools/assertrx.h"
#include "tools/fsops.h"

namespace reindexer {
namespace datastorage {

using namespace std::string_view_literals;

constexpr auto kStorageNotInitialized = "Storage is not initialized"sv;
constexpr auto kLostDirName = "lost"sv;

static void toWriteOptions(const StorageOpts& opts, leveldb::WriteOptions& wopts) noexcept { wopts.sync = opts.IsSync(); }

static void toReadOptions(const StorageOpts& opts, leveldb::ReadOptions& ropts) noexcept {
	ropts.fill_cache = opts.IsFillCache();
	ropts.verify_checksums = opts.IsVerifyChecksums();
}

LevelDbStorage::LevelDbStorage() = default;

Error LevelDbStorage::Open(const std::string& path, const StorageOpts& opts) {
	if (path.empty()) {
		return Error(errParams, "Cannot enable storage: the path is empty");
	}

	leveldb::Options options;
	options.create_if_missing = opts.IsCreateIfMissing();
	options.max_open_files = 50;
	SetDummyLogger(options);

	leveldb::DB* db = nullptr;
	leveldb::Status status = leveldb::DB::Open(options, path, &db);
	if (status.ok()) {
		db_.reset(db);
		opts_ = opts;
		dbpath_ = path;
		return Error();
	}

	return Error(errLogic, status.ToString());
}

void LevelDbStorage::Destroy(const std::string& path) {
	std::ignore = fs::RmDirAll(fs::JoinPath(path, std::string(kLostDirName)));
	leveldb::Options options;
	options.create_if_missing = true;
	db_.reset();
	leveldb::Status status = leveldb::DestroyDB(path.c_str(), options);
	if (!status.ok()) {
		fprintf(stderr, "reindexer error: unable to remove LevelDB's storage: %s, %s. Trying to remove files using backup mechanism...\n",
				path.c_str(), status.ToString().c_str());
		if (fs::RmDirAll(path) != 0) {
			fprintf(stderr, "reindexer error: unable to remove LevelDB's storage: %s, %s\n", path.c_str(), strerror(errno));
		}
	}
}

Error LevelDbStorage::Read(const StorageOpts& opts, std::string_view key, std::string& value) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}

	leveldb::ReadOptions options;
	toReadOptions(opts, options);
	leveldb::Status status = db_->Get(options, leveldb::Slice(key.data(), key.size()), &value);
	if (status.ok()) {
		return Error();
	}
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error LevelDbStorage::Write(const StorageOpts& opts, std::string_view key, std::string_view value) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}

	leveldb::WriteOptions options;
	toWriteOptions(opts, options);
	leveldb::Status status = db_->Put(options, leveldb::Slice(key.data(), key.size()), leveldb::Slice(value.data(), value.size()));
	if (status.ok()) {
		return Error();
	}
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error LevelDbStorage::Write(const StorageOpts& opts, UpdatesCollection& buffer) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}

	leveldb::WriteOptions options;
	toWriteOptions(opts, options);
	auto batchBuffer = static_cast<LevelDbBatchBuffer*>(&buffer);
	leveldb::Status status = db_->Write(options, &batchBuffer->batchWrite_);
	if (status.ok()) {
		return Error();
	}
	return Error(status.IsNotFound() ? errNotFound : errLogic, status.ToString());
}

Error LevelDbStorage::Delete(const StorageOpts& opts, std::string_view key) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}

	leveldb::WriteOptions options;
	toWriteOptions(opts, options);
	leveldb::Status status = db_->Delete(options, leveldb::Slice(key.data(), key.size()));
	if (status.ok()) {
		return Error();
	}
	return Error(errLogic, status.ToString());
}

Error LevelDbStorage::Repair(const std::string& path) {
	leveldb::Options options;
	auto status = leveldb::RepairDB(path, options);
	if (status.ok()) {
		return Error();
	}
	return Error(errLogic, status.ToString());
}

Snapshot::Ptr LevelDbStorage::MakeSnapshot() {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}
	const leveldb::Snapshot* ldbSnapshot = db_->GetSnapshot();
	assertrx(ldbSnapshot);
	return std::make_shared<LevelDbSnapshot>(ldbSnapshot);
}

void LevelDbStorage::ReleaseSnapshot(Snapshot::Ptr snapshot) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}
	if (!snapshot) {
		throw Error(errParams, "Storage pointer is null");
	}
	const LevelDbSnapshot* levelDbSnapshot = static_cast<const LevelDbSnapshot*>(snapshot.get());
	db_->ReleaseSnapshot(levelDbSnapshot->snapshot_);
	snapshot.reset();
}

Error LevelDbStorage::Flush() {
	// LevelDB does not support Flush mechanism.
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

Error LevelDbStorage::Reopen() {
	if (!dbpath_.empty()) {
		db_.reset();
		return Open(dbpath_, opts_);
	}
	return Error();
}

Cursor* LevelDbStorage::GetCursor(StorageOpts& opts) {
	if (!db_) {
		throw Error(errParams, kStorageNotInitialized);
	}
	leveldb::ReadOptions options;
	toReadOptions(opts, options);
	options.fill_cache = false;
	return new LevelDbIterator(db_->NewIterator(options));
}

UpdatesCollection* LevelDbStorage::GetUpdatesCollection() { return new LevelDbBatchBuffer(); }

std::string_view LevelDbIterator::Key() const {
	leveldb::Slice key = iterator_->key();
	return std::string_view(key.data(), key.size());
}

std::string_view LevelDbIterator::Value() const {
	leveldb::Slice key = iterator_->value();
	return std::string_view(key.data(), key.size());
}

int LevelDbComparator::Compare(std::string_view a, std::string_view b) const {
	leveldb::Options options;
	return options.comparator->Compare(leveldb::Slice(a.data(), a.size()), leveldb::Slice(b.data(), b.size()));
}

LevelDbSnapshot::LevelDbSnapshot(const leveldb::Snapshot* snapshot) noexcept : snapshot_(snapshot) {}

}  // namespace datastorage
}  // namespace reindexer

#endif	// REINDEX_WITH_LEVELDB
