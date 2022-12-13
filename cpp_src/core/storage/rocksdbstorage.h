#pragma once

#ifdef REINDEX_WITH_ROCKSDB

#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>
#include "basestorage.h"

namespace rocksdb {
class DB;
class Snapshot;
}  // namespace rocksdb

namespace reindexer {
namespace datastorage {

class RocksDbStorage : public BaseStorage {
public:
	RocksDbStorage();

	Error Read(const StorageOpts& opts, std::string_view key, std::string& value) override final;
	Error Write(const StorageOpts& opts, std::string_view key, std::string_view value) override final;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) override final;
	Error Delete(const StorageOpts& opts, std::string_view key) override final;
	Error Repair(const std::string& path) override final;

	StorageType Type() const noexcept override final { return StorageType::RocksDB; }

	Snapshot::Ptr MakeSnapshot() override final;
	void ReleaseSnapshot(Snapshot::Ptr) override final;

	Error Flush() override final;
	Error Reopen() override final;
	Cursor* GetCursor(StorageOpts& opts) override final;
	UpdatesCollection* GetUpdatesCollection() override final;

protected:
	Error doOpen(const std::string& path, const StorageOpts& opts) override final;
	void doDestroy(const std::string& path) override final;

private:
	std::string dbpath_;
	StorageOpts opts_;
	std::unique_ptr<rocksdb::DB> db_;
};

class RocksDbBatchBuffer : public UpdatesCollection {
public:
	void Put(std::string_view key, std::string_view value) override final {
		batchWrite_.Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value.data(), value.size()));
	}
	void Remove(std::string_view key) override final { batchWrite_.Delete(rocksdb::Slice(key.data(), key.size())); }
	void Clear() override final { batchWrite_.Clear(); }

private:
	rocksdb::WriteBatch batchWrite_;
	friend class RocksDbStorage;
};

class RocksDbComparator : public Comparator {
public:
	int Compare(std::string_view a, std::string_view b) const override final;
};

class RocksDbIterator : public Cursor {
public:
	RocksDbIterator(rocksdb::Iterator* iterator) noexcept : iterator_(iterator) {}

	bool Valid() const override final { return iterator_->Valid(); }
	void SeekToFirst() override final { return iterator_->SeekToFirst(); }
	void SeekToLast() override final { return iterator_->SeekToLast(); }
	void Seek(std::string_view target) override final { return iterator_->Seek(rocksdb::Slice(target.data(), target.size())); }
	void Next() override final { return iterator_->Next(); }
	void Prev() override final { return iterator_->Prev(); }

	std::string_view Key() const override final;
	std::string_view Value() const override final;

	Comparator& GetComparator() override final { return comparator_; }

private:
	const std::unique_ptr<rocksdb::Iterator> iterator_;
	RocksDbComparator comparator_;
};

class RocksDbSnapshot : public Snapshot {
public:
	RocksDbSnapshot(const rocksdb::Snapshot* snapshot) noexcept;

private:
	const rocksdb::Snapshot* snapshot_;
	friend class RocksDbStorage;
};
}  // namespace datastorage
}  // namespace reindexer

#endif	// REINDEX_WITH_ROCKSDB
