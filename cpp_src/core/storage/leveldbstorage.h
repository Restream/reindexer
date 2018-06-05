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
	Error Read(const StorageOpts& opts, const string_view& key, string& value) final;
	Error Write(const StorageOpts& opts, const string_view& key, const string_view& value) final;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) final;
	Error Delete(const StorageOpts& opts, const string_view& key) final;

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

	void Put(const string_view& key, const string_view& value) final;
	void Remove(const string_view& key) final;
	void Clear() final;

private:
	leveldb::WriteBatch batchWrite_;
	friend class LevelDbStorage;
};

class LevelDbComparator : public Comparator {
public:
	LevelDbComparator() = default;
	~LevelDbComparator() = default;

	int Compare(const string_view& a, const string_view& b) const final;
};

class LevelDbIterator : public Cursor {
public:
	LevelDbIterator(leveldb::Iterator* iterator);
	~LevelDbIterator();

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
