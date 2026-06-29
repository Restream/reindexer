#pragma once

#ifdef REINDEX_WITH_LEVELDB

#include <leveldb/env.h>
#include <leveldb/iterator.h>
#include <leveldb/write_batch.h>
#include "idatastorage.h"

namespace leveldb {
class DB;
class Snapshot;
}  // namespace leveldb

namespace reindexer {
namespace datastorage {

class [[nodiscard]] LevelDbStorage final : public IDataStorage {
public:
	LevelDbStorage();

	Error Open(const std::string& path, const StorageOpts& opts) override;
	void Destroy(const std::string& path) override;
	Error Read(const StorageOpts& opts, std::string_view key, std::string& value) override;
	Error Write(const StorageOpts& opts, std::string_view key, std::string_view value) override;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) override;
	Error Delete(const StorageOpts& opts, std::string_view key) override;
	Error Repair(const std::string& path) override;

	StorageType Type() const noexcept override { return StorageType::LevelDB; }

	Snapshot::Ptr MakeSnapshot() override;
	void ReleaseSnapshot(Snapshot::Ptr) override;

	Error Flush() override;
	Error Reopen() override;
	Cursor* GetCursor(StorageOpts& opts) override;
	UpdatesCollection* GetUpdatesCollection() override;

private:
	std::string dbpath_;
	StorageOpts opts_;
	std::unique_ptr<leveldb::DB> db_;
};

class [[nodiscard]] LevelDbBatchBuffer final : public UpdatesCollection {
public:
	void Put(std::string_view key, std::string_view value) override {
		batchWrite_.Put(leveldb::Slice(key.data(), key.size()), leveldb::Slice(value.data(), value.size()));
	}
	void Remove(std::string_view key) override { batchWrite_.Delete(leveldb::Slice(key.data(), key.size())); }
	void Clear() override { batchWrite_.Clear(); }
	void Id(uint64_t id) noexcept override { batchId_ = id; }
	uint64_t Id() const noexcept override { return batchId_; }

private:
	leveldb::WriteBatch batchWrite_;
	friend class LevelDbStorage;
	uint64_t batchId_;
};

class [[nodiscard]] LevelDbComparator : public Comparator {
public:
	int Compare(std::string_view a, std::string_view b) const override final;
};

class [[nodiscard]] LevelDbIterator final : public Cursor {
public:
	explicit LevelDbIterator(leveldb::Iterator* iterator) noexcept : iterator_(iterator) {}

	bool Valid() const override { return iterator_->Valid(); }
	void SeekToFirst() override { return iterator_->SeekToFirst(); }
	void SeekToLast() override { return iterator_->SeekToLast(); }
	void Seek(std::string_view target) override { return iterator_->Seek(leveldb::Slice(target.data(), target.size())); }
	void Next() override { return iterator_->Next(); }
	void Prev() override { return iterator_->Prev(); }

	std::string_view Key() const override;
	std::string_view Value() const override;

	Comparator& GetComparator() override { return comparator_; }

private:
	const std::unique_ptr<leveldb::Iterator> iterator_;
	LevelDbComparator comparator_;
};

class [[nodiscard]] LevelDbSnapshot final : public Snapshot {
public:
	explicit LevelDbSnapshot(const leveldb::Snapshot* snapshot) noexcept;

private:
	const leveldb::Snapshot* snapshot_;
	friend class LevelDbStorage;
};
}  // namespace datastorage
}  // namespace reindexer

#endif	// REINDEX_WITH_LEVELDB
