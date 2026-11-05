#pragma once

#ifdef REINDEX_WITH_ROCKSDB

#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>
#include "idatastorage.h"

namespace rocksdb {
class DB;
class Snapshot;
}  // namespace rocksdb

namespace reindexer {
namespace datastorage {

class [[nodiscard]] RocksDbStorage final : public IDataStorage {
public:
	RocksDbStorage();

	Error Open(const std::string& path, const StorageOpts& opts) override;
	void Destroy(const std::string& path) override;
	Error Read(const StorageOpts& opts, std::string_view key, std::string& value) override;
	Error Write(const StorageOpts& opts, std::string_view key, std::string_view value) override;
	Error Write(const StorageOpts& opts, UpdatesCollection& buffer) override;
	Error Delete(const StorageOpts& opts, std::string_view key) override;
	Error Repair(const std::string& path) override;

	StorageType Type() const noexcept override { return StorageType::RocksDB; }

	Snapshot::Ptr MakeSnapshot() override;
	void ReleaseSnapshot(Snapshot::Ptr) override;

	Error Flush() override;
	Error Reopen() override;
	Cursor* GetCursor(StorageOpts& opts) override;
	UpdatesCollection* GetUpdatesCollection() override;

private:
	std::string dbpath_;
	StorageOpts opts_;
	std::unique_ptr<rocksdb::DB> db_;
};

class [[nodiscard]] RocksDbBatchBuffer final : public UpdatesCollection {
public:
	void Put(std::string_view key, std::string_view value) override {
		batchWrite_.Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value.data(), value.size()));
	}
	void Remove(std::string_view key) override { batchWrite_.Delete(rocksdb::Slice(key.data(), key.size())); }
	void Clear() override { batchWrite_.Clear(); }
	void Id(uint64_t id) noexcept override { batchId_ = id; }
	uint64_t Id() const noexcept override { return batchId_; }

private:
	rocksdb::WriteBatch batchWrite_;
	friend class RocksDbStorage;
	uint64_t batchId_;
};

class [[nodiscard]] RocksDbComparator : public Comparator {
public:
	int Compare(std::string_view a, std::string_view b) const override final;
};

class [[nodiscard]] RocksDbIterator final : public Cursor {
public:
	explicit RocksDbIterator(rocksdb::Iterator* iterator) noexcept : iterator_(iterator) {}

	bool Valid() const override { return iterator_->Valid(); }
	void SeekToFirst() override { return iterator_->SeekToFirst(); }
	void SeekToLast() override { return iterator_->SeekToLast(); }
	void Seek(std::string_view target) override { return iterator_->Seek(rocksdb::Slice(target.data(), target.size())); }
	void Next() override { return iterator_->Next(); }
	void Prev() override { return iterator_->Prev(); }

	std::string_view Key() const override;
	std::string_view Value() const override;

	Comparator& GetComparator() override { return comparator_; }

private:
	const std::unique_ptr<rocksdb::Iterator> iterator_;
	RocksDbComparator comparator_;
};

class [[nodiscard]] RocksDbSnapshot final : public Snapshot {
public:
	explicit RocksDbSnapshot(const rocksdb::Snapshot* snapshot) noexcept;

private:
	const rocksdb::Snapshot* snapshot_;
	friend class RocksDbStorage;
};
}  // namespace datastorage
}  // namespace reindexer

#endif	// REINDEX_WITH_ROCKSDB
