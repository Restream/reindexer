#pragma once

#ifdef REINDEX_WITH_LEVELDB

#include <leveldb/write_batch.h>
#include "basestorage.h"

using std::unique_ptr;

namespace leveldb {
class DB;
class Snapshot;
class Iterator;
}  // namespace leveldb

namespace reindexer {
namespace datastorage {

class LevelDbStorage : public BaseStorage {
public:
	LevelDbStorage();
	~LevelDbStorage();

	Error Read(const StorageOpts& opts, std::string_view key, string& value) override final;
	Error Write(const StorageOpts& opts, std::string_view key, std::string_view value) override final;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) override final;
	Error Delete(const StorageOpts& opts, std::string_view key) override final;
	Error Repair(const string& path) override final;

	StorageType Type() const noexcept override final { return StorageType::LevelDB; }

	Snapshot::Ptr MakeSnapshot() override final;
	void ReleaseSnapshot(Snapshot::Ptr) override final;

	void Flush() final;
	Cursor* GetCursor(StorageOpts& opts) override final;
	UpdatesCollection* GetUpdatesCollection() override final;

protected:
	Error doOpen(const string& path, const StorageOpts& opts) override final;
	void doDestroy(const string& path) override final;

private:
	string dbpath_;
	StorageOpts opts_;
	unique_ptr<leveldb::DB> db_;
};

class LevelDbBatchBuffer : public UpdatesCollection {
public:
	LevelDbBatchBuffer();
	~LevelDbBatchBuffer();

	void Put(std::string_view key, std::string_view value) override final;
	void Remove(std::string_view key) override final;
	void Clear() override final;

private:
	leveldb::WriteBatch batchWrite_;
	friend class LevelDbStorage;
};

class LevelDbComparator : public Comparator {
public:
	LevelDbComparator() = default;
	~LevelDbComparator() = default;

	int Compare(std::string_view a, std::string_view b) const override final;
};

class LevelDbIterator : public Cursor {
public:
	LevelDbIterator(leveldb::Iterator* iterator);
	~LevelDbIterator();

	bool Valid() const override final;
	void SeekToFirst() override final;
	void SeekToLast() override final;
	void Seek(std::string_view target) override final;
	void Next() override final;
	void Prev() override final;

	std::string_view Key() const override final;
	std::string_view Value() const override final;

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

#endif	// REINDEX_WITH_LEVELDB
