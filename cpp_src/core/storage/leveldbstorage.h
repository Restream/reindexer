#pragma once

#include <leveldb/write_batch.h>
#include "idatastorage.h"

using std::unique_ptr;

namespace leveldb {
class DB;
class Snapshot;
class Iterator;
}  // namespace leveldb

namespace reindexer {
namespace datastorage {

class LevelDbStorage : public IDataStorage {
public:
	LevelDbStorage();
	~LevelDbStorage();

	Error Open(const string& path, const StorageOpts& opts) final;
	Error Read(const StorageOpts& opts, const Slice& key, string& value) final;
	Error Write(const StorageOpts& opts, const Slice& key, const Slice& value) final;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) final;
	Error Delete(const StorageOpts& opts, const Slice& key) final;

	Snapshot::Ptr MakeSnapshot() final;
	void ReleaseSnapshot(Snapshot::Ptr) final;

	void Flush() final;
	void Destroy(const string& path) final;
	Cursor* GetCursor(StorageOpts& opts) final;
	UpdatesCollection* GetUpdatesCollection() final;

private:
	string dbpath_;
	StorageOpts opts_;
	shared_ptr<leveldb::DB> db_;
};

class LevelDbBatchBuffer : public UpdatesCollection {
public:
	LevelDbBatchBuffer();
	~LevelDbBatchBuffer();

	void Put(const Slice& key, const Slice& value) final;
	void Remove(const Slice& key) final;
	void Clear() final;

private:
	leveldb::WriteBatch batchWrite_;
	friend class LevelDbStorage;
};

class LevelDbComparator : public Comparator {
public:
	LevelDbComparator() = default;
	~LevelDbComparator() = default;

	int Compare(const Slice& a, const Slice& b) const final;
};

class LevelDbIterator : public Cursor {
public:
	LevelDbIterator(leveldb::Iterator* iterator);
	~LevelDbIterator();

	bool Valid() const final;
	void SeekToFirst() final;
	void SeekToLast() final;
	void Seek(const Slice& target) final;
	void Next() final;
	void Prev() final;

	Slice Key() const final;
	Slice Value() const final;

	Comparator& GetComparator() final;

private:
	const unique_ptr<leveldb::Iterator> iterator_;
	LevelDbComparator comparator_;
};

class LevelDbSnapshot : public Snapshot {
public:
	LevelDbSnapshot(const leveldb::Snapshot* snapshot);
	~LevelDbSnapshot();

private:
	const leveldb::Snapshot* snapshot_;
	friend class LevelDbStorage;
};
}  // namespace datastorage
}  // namespace reindexer
