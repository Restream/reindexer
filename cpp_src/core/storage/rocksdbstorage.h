#pragma once

#ifdef REINDEX_WITH_ROCKSDB

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include "idatastorage.h"

using std::unique_ptr;

namespace reindexer {
namespace datastorage {

class RocksDbStorage : public IDataStorage {
public:
	RocksDbStorage();
	~RocksDbStorage();

	Error Open(const string& path, const StorageOpts& opts) final;
	Error Read(const StorageOpts& opts, const string_view& key, string& value) final;
	Error Write(const StorageOpts& opts, const string_view& key, const string_view& value) final;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) final;
	Error Delete(const StorageOpts& opts, const string_view& key) final;
	Error Repair(const string& path) final;

	Snapshot::Ptr MakeSnapshot() final;
	void ReleaseSnapshot(Snapshot::Ptr) final;

	void Flush() final;
	void Destroy(const string& path) final;
	Cursor* GetCursor(StorageOpts& opts) final;
	UpdatesCollection* GetUpdatesCollection() final;

private:
	string dbpath_;
	StorageOpts opts_;
	shared_ptr<rocksdb::DB> db_;
};

class RocksDbBatchBuffer : public UpdatesCollection {
public:
	RocksDbBatchBuffer();
	~RocksDbBatchBuffer();

	void Put(const string_view& key, const string_view& value) final;
	void Remove(const string_view& key) final;
	void Clear() final;

private:
	rocksdb::WriteBatch batchWrite_;
	friend class RocksDbStorage;
};

class RocksDbComparator : public Comparator {
public:
	RocksDbComparator() = default;
	~RocksDbComparator() = default;

	int Compare(const string_view& a, const string_view& b) const final;
};

class RocksDbIterator : public Cursor {
public:
	RocksDbIterator(rocksdb::Iterator* iterator);
	~RocksDbIterator();

	bool Valid() const final;
	void SeekToFirst() final;
	void SeekToLast() final;
	void Seek(const string_view& target) final;
	void Next() final;
	void Prev() final;

	string_view Key() const final;
	string_view Value() const final;

	Comparator& GetComparator() final;

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
