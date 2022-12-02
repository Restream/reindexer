#pragma once

#ifdef REINDEX_WITH_LEVELDB

#include <leveldb/iterator.h>
#include <leveldb/write_batch.h>
#include "basestorage.h"

namespace leveldb {
class DB;
class Snapshot;
}  // namespace leveldb

namespace reindexer {
namespace datastorage {

class LevelDbStorage : public BaseStorage {
public:
	LevelDbStorage();

	Error Read(const StorageOpts& opts, std::string_view key, std::string& value) override final;
	Error Write(const StorageOpts& opts, std::string_view key, std::string_view value) override final;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) override final;
	Error Delete(const StorageOpts& opts, std::string_view key) override final;
	Error Repair(const std::string& path) override final;

	StorageType Type() const noexcept override final { return StorageType::LevelDB; }

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
	std::unique_ptr<leveldb::DB> db_;
};

class LevelDbBatchBuffer : public UpdatesCollection {
public:
	void Put(std::string_view key, std::string_view value) override final {
		batchWrite_.Put(leveldb::Slice(key.data(), key.size()), leveldb::Slice(value.data(), value.size()));
	}
	void Remove(std::string_view key) override final { batchWrite_.Delete(leveldb::Slice(key.data(), key.size())); }
	void Clear() override final { batchWrite_.Clear(); }

private:
	leveldb::WriteBatch batchWrite_;
	friend class LevelDbStorage;
};

class LevelDbComparator : public Comparator {
public:
	int Compare(std::string_view a, std::string_view b) const override final;
};

class LevelDbIterator : public Cursor {
public:
	LevelDbIterator(leveldb::Iterator* iterator) noexcept : iterator_(iterator) {}

	bool Valid() const override final { return iterator_->Valid(); }
	void SeekToFirst() override final { return iterator_->SeekToFirst(); }
	void SeekToLast() override final { return iterator_->SeekToLast(); }
	void Seek(std::string_view target) override final { return iterator_->Seek(leveldb::Slice(target.data(), target.size())); }
	void Next() override final { return iterator_->Next(); }
	void Prev() override final { return iterator_->Prev(); }

	std::string_view Key() const override final;
	std::string_view Value() const override final;

	Comparator& GetComparator() final { return comparator_; }

private:
	const std::unique_ptr<leveldb::Iterator> iterator_;
	LevelDbComparator comparator_;
};

class LevelDbSnapshot : public Snapshot {
public:
	LevelDbSnapshot(const leveldb::Snapshot* snapshot) noexcept;

private:
	const leveldb::Snapshot* snapshot_;
	friend class LevelDbStorage;
};
}  // namespace datastorage
}  // namespace reindexer

#endif	// REINDEX_WITH_LEVELDB
