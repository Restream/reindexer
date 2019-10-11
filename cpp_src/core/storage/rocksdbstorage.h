#pragma once

#ifdef REINDEX_WITH_ROCKSDB

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include "basestorage.h"

using std::unique_ptr;

namespace reindexer {
namespace datastorage {

class RocksDbStorage : public BaseStorage {
public:
	RocksDbStorage();
	~RocksDbStorage();

	Error Read(const StorageOpts& opts, const string_view& key, string& value) override final;
	Error Write(const StorageOpts& opts, const string_view& key, const string_view& value) override final;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) override final;
	Error Delete(const StorageOpts& opts, const string_view& key) override final;
	Error Repair(const string& path) override final;

	StorageType Type() const noexcept override final { return StorageType::RocksDB; }

	Snapshot::Ptr MakeSnapshot() override final;
	void ReleaseSnapshot(Snapshot::Ptr) override final;

	void Flush() override final;
	Cursor* GetCursor(StorageOpts& opts) override final;
	UpdatesCollection* GetUpdatesCollection() override final;

protected:
	Error doOpen(const string& path, const StorageOpts& opts) override final;
	void doDestroy(const string& path) override final;

private:
	string dbpath_;
	StorageOpts opts_;
	unique_ptr<rocksdb::DB> db_;
};

class RocksDbBatchBuffer : public UpdatesCollection {
public:
	RocksDbBatchBuffer();
	~RocksDbBatchBuffer();

	void Put(const string_view& key, const string_view& value) override final;
	void Remove(const string_view& key) override final;
	void Clear() override final;

private:
	rocksdb::WriteBatch batchWrite_;
	friend class RocksDbStorage;
};

class RocksDbComparator : public Comparator {
public:
	RocksDbComparator() = default;
	~RocksDbComparator() = default;

	int Compare(const string_view& a, const string_view& b) const override final;
};

class RocksDbIterator : public Cursor {
public:
	RocksDbIterator(rocksdb::Iterator* iterator);
	~RocksDbIterator();

	bool Valid() const override final;
	void SeekToFirst() override final;
	void SeekToLast() override final;
	void Seek(const string_view& target) override final;
	void Next() override final;
	void Prev() override final;

	string_view Key() const override final;
	string_view Value() const override final;

	Comparator& GetComparator() override final;

private:
	const unique_ptr<rocksdb::Iterator> iterator_;
	RocksDbComparator comparator_;
};

class RocksDbSnapshot : public Snapshot {
public:
	RocksDbSnapshot(const rocksdb::Snapshot* snapshot);
	~RocksDbSnapshot();

private:
	const rocksdb::Snapshot* snapshot_;
	friend class RocksDbStorage;
};
}  // namespace datastorage
}  // namespace reindexer

#endif  // REINDEX_WITH_ROCKSDB
