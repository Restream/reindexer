#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/tagsmatcher.h"
#include "core/dbconfig.h"
#include "core/function/precomputed_values.h"
#include "core/namespace/asyncstorage.h"
#include "core/namespace/migrations/pk_migration_service.h"
#include "core/namespace/namespaceimpl.h"
#include "core/namespace/namespacename.h"
#include "core/namespace/sys_records_storage.h"
#include "core/query/query_impl.h"
#include "core/queryresults/localqueryresults.h"
#include "core/storage/storage_prefixes.h"
#include "gtest/gtest.h"
#include "tools/catch_and_return.h"
#include "tools/scope_guard.h"
#include "tools/serilize/serializer.h"
#include "tools/serilize/wrserializer.h"
#include "vendor/gason/gason.h"

namespace reindexer_tests {

/** @brief Installs a fault-injecting backend before the namespace is used; no production storage hooks are exposed. */
class [[nodiscard]] NamespaceStorageTestAccess {
public:
	static void AttachStorage(reindexer::NamespaceImpl& ns, std::shared_ptr<reindexer::datastorage::IDataStorage> backend) {
		auto& storage = ns.storage_;
		assertrx(!storage.storage_);
		storage.storage_ = std::move(backend);
		storage.curUpdatesChunck_ = storage.createUpdatesCollection();
		storage.updateStatusCache();
	}
	static void RecoverStorage(reindexer::NamespaceImpl& ns) { ns.storage_.Flush(reindexer::StorageFlushOpts{}.WithImmediateReopen()); }
	static unsigned ReplStateUpdates(const reindexer::NamespaceImpl& ns) { return ns.replStateUpdates_.load(); }
	static bool MigrateToCurrentPK(reindexer::NamespaceImpl& ns) {
		const auto* pk = ns.pkFields();
		assertrx(pk);
		return reindexer::migrations::PKMigrationService{ns}.MigrateToNewPK(*pk);
	}
	static void Update(reindexer::NamespaceImpl& ns, const reindexer::Query& query, const reindexer::RdxContext& rdxCtx) {
		reindexer::Error storageError;
		const reindexer::NsContext ctx{rdxCtx, storageError};
		auto lock = ns.dataWLock(rdxCtx);
		reindexer::LocalQueryResults result;
		constexpr bool noLock = true;
		result.AddNamespace(reindexer::NamespaceImpl::Ptr{&ns}, noLock);
		reindexer::UpdatesContainer updates;
		ns.doUpdateTr(result, updates, reindexer::Impl(query), ctx, reindexer::functions::PrecomputedValues{});
		constexpr bool tryForceFlush = false;
		ns.replicate(std::move(updates), std::move(lock), tryForceFlush, nullptr, ctx);
	}
};

namespace {

using namespace reindexer;
using SysRecordKey = SysRecordsStorage::Key;
using namespace std::chrono_literals;

class [[nodiscard]] RecordBatch final : public datastorage::UpdatesCollection {
public:
	void Put(std::string_view key, std::string_view value) override { entries.emplace_back(std::string{key}, std::string{value}); }
	void Remove(std::string_view key) override { entries.emplace_back(std::string{key}, std::nullopt); }
	void Clear() override { entries.clear(); }
	void Id(uint64_t id) noexcept override { id_ = id; }
	uint64_t Id() const noexcept override { return id_; }
	std::vector<std::pair<std::string, std::optional<std::string>>> entries;

private:
	uint64_t id_ = 0;
};

/** @brief Models atomic batches and loss of writes that were not covered by a successful Sync. */
class [[nodiscard]] RecordStorage final : public datastorage::IDataStorage {
public:
	enum class [[nodiscard]] Failure { None, BeforeWrite, AfterWrite };

	Error Open(const std::string&, const StorageOpts&) noexcept override { return Error(); }
	Error Read(const StorageOpts&, std::string_view key, std::string& value) noexcept override {
		try {
			std::lock_guard lock{mutex_};
			if (key == readErrorKey_) {
				return Error(errLogic, "Injected read error");
			}
			if (const auto it = data_.find(key); it != data_.end()) {
				value = it->second;
				return Error();
			}
			return Error(errNotFound, "Missing test key");
		} CATCH_AND_RETURN
	}
	Error Write(const StorageOpts& opts, std::string_view key, std::string_view value) noexcept override {
		try {
			RecordBatch batch;
			batch.Put(key, value);
			return Write(opts, batch);
		} CATCH_AND_RETURN
	}
	Error Write(const StorageOpts& opts, datastorage::UpdatesCollection& updates) noexcept override {
		try {
			std::function<void()> beforeWrite;
			{
				std::lock_guard lock{mutex_};
				beforeWrite = std::exchange(beforeWrite_, {});
			}
			if (beforeWrite) {
				beforeWrite();
			}
			std::lock_guard lock{mutex_};
			++attempts_;
			const auto failure = std::exchange(failure_, Failure::None);
			if (failure == Failure::BeforeWrite) {
				return Error(errLogic, "Injected error before atomic write");
			}
			const auto& entries = dynamic_cast<RecordBatch&>(updates).entries;
			if (!writeErrorPrefix_.empty() && std::any_of(entries.begin(), entries.end(), [this](const auto& entry) {
					return entry.first.starts_with(writeErrorPrefix_);
				})) {
				return Error(errLogic, "Injected key write error");
			}
			auto next = data_;
			for (const auto& [key, value] : entries) {
				if (value) {
					next.insert_or_assign(key, *value);
				} else {
					next.erase(key);
				}
			}
			data_.swap(next);
			if (opts.IsSync()) {
				durable_ = data_;
			}
			if (failure == Failure::AfterWrite) {
				return Error(errLogic, "Injected error after atomic write");
			}
			return Error();
		} CATCH_AND_RETURN
	}
	Error Delete(const StorageOpts& opts, std::string_view key) noexcept override {
		try {
			RecordBatch batch;
			batch.Remove(key);
			return Write(opts, batch);
		} CATCH_AND_RETURN
	}
	datastorage::Snapshot::Ptr MakeSnapshot() override { return {}; }
	void ReleaseSnapshot(datastorage::Snapshot::Ptr) override {}
	Error Flush() noexcept override { return Error(); }
	Error Reopen() noexcept override {
		std::lock_guard lock{mutex_};
		++reopenAttempts_;
		return Error();
	}
	datastorage::Cursor* GetCursor(StorageOpts&) override { return nullptr; }
	datastorage::UpdatesCollection* GetUpdatesCollection() override { return new RecordBatch; }
	void Destroy(const std::string&) override {}
	Error Repair(const std::string&) noexcept override { return Error(); }
	datastorage::StorageType Type() const noexcept override { return datastorage::StorageType::LevelDB; }

	void FailNextWrite(Failure failure) {
		std::lock_guard lock{mutex_};
		failure_ = failure;
	}
	void FailRead(std::string key) {
		std::lock_guard lock{mutex_};
		readErrorKey_ = std::move(key);
	}
	void FailWritesWithPrefix(std::string prefix) {
		std::lock_guard lock{mutex_};
		writeErrorPrefix_ = std::move(prefix);
	}
	void BeforeNextWrite(std::function<void()> fn) {
		std::lock_guard lock{mutex_};
		beforeWrite_ = std::move(fn);
	}
	void LoseUnsyncedWrites() {
		std::lock_guard lock{mutex_};
		data_ = durable_;
	}
	size_t CountKeys(std::string_view prefix) const {
		std::lock_guard lock{mutex_};
		return std::count_if(data_.begin(), data_.end(), [prefix](const auto& entry) { return entry.first.starts_with(prefix); });
	}
	size_t Attempts() const {
		std::lock_guard lock{mutex_};
		return attempts_;
	}
	size_t ReopenAttempts() const {
		std::lock_guard lock{mutex_};
		return reopenAttempts_;
	}

private:
	mutable std::mutex mutex_;
	std::map<std::string, std::string, std::less<>> data_;
	decltype(data_) durable_;
	Failure failure_ = Failure::None;
	std::string readErrorKey_;
	std::string writeErrorPrefix_;
	std::function<void()> beforeWrite_;
	size_t attempts_ = 0;
	size_t reopenAttempts_ = 0;
};

class [[nodiscard]] StorageTestReplication final : public cluster::IDataReplicator, public cluster::IDataSyncer {
public:
	Error Replicate(UpdatesContainer&& records, const std::function<void()>&, const RdxContext&) noexcept override {
		try {
			for (const auto& record : records) {
				updates.push_back(record.Type());
			}
			return Error();
		} CATCH_AND_RETURN
	}
	Error ReplicateAsync(UpdatesContainer&& records, const RdxContext& ctx) noexcept override {
		return Replicate(std::move(records), {}, ctx);
	}
	void AwaitInitialSync(const NamespaceName&, const RdxContext&) const override {}
	void AwaitInitialSync(const RdxContext&) const override {}
	bool IsInitialSyncDone(const NamespaceName&) const override { return true; }
	bool IsInitialSyncDone() const override { return true; }
	std::vector<updates::URType> updates;
};

class [[nodiscard]] NamespaceStorageFailureTest : public ::testing::Test {
protected:
	void SetUp() override {
		ns_ = make_intrusive<NamespaceImpl>("storage_failure_test", std::nullopt, replication_, observers_, embeddersCache_);
		NamespaceStorageTestAccess::AttachStorage(*ns_, backend_);
		ASSERT_NO_THROW(ns_->AddIndex(IndexDef{"id", "hash", "int", IndexOpts{}.PK()}, ctx_));
		replication_.updates.clear();
	}
	void TearDown() override {
		if (ns_) {
			ASSERT_NO_THROW(NamespaceStorageTestAccess::RecoverStorage(*ns_));
			ns_.reset();
		}
	}
	void upsert(std::string_view json) {
		auto item = ns_->NewItem(ctx_);
		const auto err = item.FromJSON(json);
		if (!err.ok()) {
			throw err;
		}
		ns_->Upsert(item, ctx_);
	}
	void expectReplicated(updates::URType type) {
		EXPECT_NE(std::find(replication_.updates.begin(), replication_.updates.end(), type), replication_.updates.end());
		replication_.updates.clear();
	}
	void destroyAndExpectFlushed() {
		WrSerializer replState;
		JsonBuilder builder{replState};
		ns_->GetReplState(ctx_).GetJSON(builder);
		builder.End();

		// Destroy with pending writes: recovering storage beforehand would hide a skipped destructor flush.
		ns_.reset();
		EXPECT_EQ(backend_->ReopenAttempts(), 1);
		EXPECT_EQ(backend_->CountKeys(kRxStorageItemPrefix), 1);
		std::string content;
		ASSERT_TRUE(backend_->Read(StorageOpts{}, "repl", content).ok());
		EXPECT_EQ(content, replState.Slice());
		ASSERT_TRUE(backend_->Read(StorageOpts{}, "tags", content).ok());
		TagsMatcher tags;
		Serializer ser{content};
		ASSERT_NO_THROW(tags.deserialize(ser));
		EXPECT_FALSE(tags.name2tag("new_field").IsEmpty());
	}
	StorageTestReplication replication_;
	UpdatesObservers observers_{"storage_failure_db", replication_, 1 << 20};
	std::shared_ptr<EmbeddersCache> embeddersCache_;
	std::shared_ptr<RecordStorage> backend_ = std::make_shared<RecordStorage>();
	NamespaceImpl::Ptr ns_;
	RdxContext ctx_;
};

TEST_F(NamespaceStorageFailureTest, AppliedChangesAreQueuedAndReplicatedBeforeReportingStorageError) {
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	EXPECT_THROW(upsert(R"json({"id":1,"new_field":10})json"), Error);
	EXPECT_EQ(ns_->GetItemsCount(), 1);
	expectReplicated(updates::URType::ItemUpsert);
	EXPECT_EQ(backend_->CountKeys(kRxStorageItemPrefix), 0);
	ASSERT_NO_THROW(NamespaceStorageTestAccess::RecoverStorage(*ns_));
	EXPECT_EQ(backend_->CountKeys(kRxStorageItemPrefix), 1);

	backend_->FailNextWrite(RecordStorage::Failure::AfterWrite);
	EXPECT_THROW(NamespaceStorageTestAccess::Update(*ns_, Query{"storage_failure_test"}.Set("another_field", 20), ctx_), Error);
	expectReplicated(updates::URType::ItemUpdate);
	ASSERT_NO_THROW(NamespaceStorageTestAccess::RecoverStorage(*ns_));

	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	EXPECT_THROW(ns_->AddIndex(IndexDef{"new_field", "hash", "int", IndexOpts{}}, ctx_), Error);
	expectReplicated(updates::URType::IndexAdd);
	const auto definition = ns_->GetDefinition(ctx_);
	EXPECT_TRUE(std::any_of(definition.indexes.begin(), definition.indexes.end(),
							[](const IndexDef& index) { return index.Name() == "new_field"; }));
	ASSERT_NO_THROW(NamespaceStorageTestAccess::RecoverStorage(*ns_));
}

TEST_F(NamespaceStorageFailureTest, ReplicationStateWriteFailureDoesNotBlockTtl) {
	ASSERT_NO_THROW(ns_->AddIndex(IndexDef{"expires", {"expires"}, "ttl", "int64", IndexOpts{}, 1}, ctx_));
	ASSERT_NO_THROW(upsert(R"json({"id":1,"expires":1})json"));
	// Changing server ID schedules replication state for background persistence.
	DBConfigProvider config;
	gason::JsonParser parser;
	const auto err = config.FromJSON(parser.Parse(R"json({"replication":{"server_id":1}})json"));
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_NO_THROW(ns_->OnConfigUpdated(config, ctx_));
	ASSERT_GT(NamespaceStorageTestAccess::ReplStateUpdates(*ns_), 0);
	replication_.updates.clear();
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	EXPECT_NO_THROW(ns_->BackgroundRoutine(nullptr));
	EXPECT_EQ(ns_->GetItemsCount(), 0);
	expectReplicated(updates::URType::ItemDelete);
	EXPECT_EQ(backend_->ReopenAttempts(), 0);
	EXPECT_EQ(NamespaceStorageTestAccess::ReplStateUpdates(*ns_), 0);
	const auto attempts = backend_->Attempts();
	EXPECT_NO_THROW(ns_->BackgroundRoutine(nullptr));
	EXPECT_EQ(backend_->Attempts(), attempts);
	ASSERT_NO_THROW(NamespaceStorageTestAccess::RecoverStorage(*ns_));
	EXPECT_EQ(backend_->CountKeys(kRxStorageItemPrefix), 0);
}

TEST_F(NamespaceStorageFailureTest, DestructorFlushesAfterPreviousStorageError) {
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	EXPECT_THROW(upsert(R"json({"id":1,"new_field":10})json"), Error);
	ASSERT_EQ(backend_->CountKeys(kRxStorageItemPrefix), 0);
	ASSERT_EQ(backend_->ReopenAttempts(), 0);
	destroyAndExpectFlushed();
}

TEST_F(NamespaceStorageFailureTest, DestructorFlushesAfterReplicationStateWriteError) {
	ASSERT_NO_THROW(upsert(R"json({"id":1,"new_field":10})json"));
	ASSERT_EQ(backend_->CountKeys(kRxStorageItemPrefix), 0);
	ASSERT_EQ(backend_->ReopenAttempts(), 0);
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	destroyAndExpectFlushed();
}

TEST_F(NamespaceStorageFailureTest, PkMigrationKeepsIncompleteStatusAfterLegacySlotMigration) {
	ASSERT_NO_THROW(upsert(R"json({"id":1})json"));
	WrSerializer legacy;
	legacy.PutUInt64(11);
	legacy.PutUInt8(1);
	ASSERT_TRUE(backend_->Write(StorageOpts{}.Sync(), "pk_migration_status.3", legacy.Slice()).ok());

	backend_->FailWritesWithPrefix(kRxStorageItemPrefix);
	auto resetFailures = MakeScopeGuard([this] {
		backend_->FailRead({});
		backend_->FailWritesWithPrefix({});
	});
	// Snapshot application bypasses the public DDL check that would otherwise read the migration status first.
	backend_->FailRead("pk_migration_status.3");
	const auto attempts = backend_->Attempts();
	EXPECT_FALSE(NamespaceStorageTestAccess::MigrateToCurrentPK(*ns_));
	ASSERT_EQ(backend_->Attempts(), attempts);
	backend_->FailRead({});

	// The incomplete marker is durable, then flushing the queued item fails before any cleanup can run.
	EXPECT_FALSE(NamespaceStorageTestAccess::MigrateToCurrentPK(*ns_));
	EXPECT_EQ(backend_->CountKeys(kRxStorageItemPrefix), 0);
	std::string value;
	ASSERT_TRUE(backend_->Read(StorageOpts{}, "pk_migration_status", value).ok());
	EXPECT_EQ(value, std::string(1, '\0'));
	for (size_t i = 0; i < 8; ++i) {
		EXPECT_EQ(backend_->Read(StorageOpts{}, "pk_migration_status." + std::to_string(i), value).code(), errNotFound);
	}

	backend_->LoseUnsyncedWrites();
	AsyncStorage reopened{backend_};
	const auto name = ns_->GetName(ctx_);
	const auto persisted = SysRecordsStorage{reopened, name}.Read(SysRecordKey::PkMigrationStatus);
	ASSERT_TRUE(persisted.status.ok()) << persisted.status.what();
	EXPECT_EQ(persisted.content, std::string(1, '\0'));
	EXPECT_TRUE(migrations::PKMigrationService{*ns_}.HasIncompleteMigration());
}

class [[nodiscard]] AsyncStorageRecordsTest : public ::testing::Test {
protected:
	SysRecordsStorage records() { return SysRecordsStorage{storage_, name_}; }
	void expectBody(std::string_view key, std::string_view body) {
		std::string value;
		const auto err = backend_->Read(StorageOpts{}, key, value);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(value, body);
	}
	void seedLegacy() {
		WrSerializer ser;
		ser.PutUInt64(10);
		ser.Write("latest");
		ASSERT_TRUE(backend_->Write(StorageOpts{}.Sync(), "tags.2", ser.Slice()).ok());
		ASSERT_TRUE(backend_->Write(StorageOpts{}.Sync(), "tags", "stale").ok());
	}
	NamespaceName name_{"records_test"};
	std::shared_ptr<RecordStorage> backend_ = std::make_shared<RecordStorage>();
	AsyncStorage storage_{backend_};
};

TEST_F(AsyncStorageRecordsTest, SyncRecordSurvivesLossOfUnsyncedWrites) {
	records().Write("durable", SysRecordKey::TagsMatcher);
	storage_.Write("item", "buffered");
	storage_.Flush(StorageFlushOpts{});
	backend_->LoseUnsyncedWrites();
	expectBody("tags", "durable");
	std::string value;
	EXPECT_EQ(backend_->Read(StorageOpts{}, "item", value).code(), errNotFound);
}

TEST_F(AsyncStorageRecordsTest, FailedRecordWritePropagatesAndBlocksDependentItems) {
	records().Write("old", SysRecordKey::TagsMatcher);
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	EXPECT_THROW(records().Write("new", SysRecordKey::TagsMatcher), Error);
	const auto attempts = backend_->Attempts();
	EXPECT_THROW(records().Write("latest", SysRecordKey::TagsMatcher), Error);
	EXPECT_THROW(records().Write("new-schema", SysRecordKey::Schema), Error);
	EXPECT_EQ(backend_->Attempts(), attempts);
	EXPECT_EQ(backend_->ReopenAttempts(), 0);
	expectBody("tags", "old");
	EXPECT_FALSE(records().Read(SysRecordKey::TagsMatcher).status.ok());
	storage_.Write("item", "depends-on-new-tags");
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	EXPECT_THROW(storage_.Flush(StorageFlushOpts{}.WithImmediateReopen()), Error);
	std::string value;
	EXPECT_EQ(backend_->Read(StorageOpts{}, "item", value).code(), errNotFound);
	storage_.Flush(StorageFlushOpts{}.WithImmediateReopen());
	EXPECT_EQ(backend_->ReopenAttempts(), 2);
	expectBody("tags", "latest");
	expectBody("schema", "new-schema");
	expectBody("item", "depends-on-new-tags");
	backend_->LoseUnsyncedWrites();
	expectBody("tags", "latest");
	expectBody("schema", "new-schema");
}

TEST_F(AsyncStorageRecordsTest, RelatedMetadataDoesNotPartiallyCommitOnFailure) {
	records().Write("old-indexes", SysRecordKey::Indexes);
	records().Write("old-tags", SysRecordKey::TagsMatcher);
	const std::array updates{SysRecordsStorage::Record{.key = SysRecordKey::Indexes, .value = "new-indexes"},
							 SysRecordsStorage::Record{.key = SysRecordKey::TagsMatcher, .value = "new-tags"}};
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	EXPECT_THROW(records().Write(updates), Error);
	backend_->LoseUnsyncedWrites();
	expectBody("indexes", "old-indexes");
	expectBody("tags", "old-tags");
}

TEST_F(AsyncStorageRecordsTest, InterruptedMigrationBeforeWriteCanBeRetried) {
	seedLegacy();
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	const auto failed = records().Read(SysRecordKey::TagsMatcher);
	EXPECT_EQ(failed.status.code(), errLogic);
	EXPECT_STREQ(failed.status.what(), "Injected error before atomic write");
	EXPECT_TRUE(failed.content.empty());
	EXPECT_FALSE(records().Read(SysRecordKey::TagsMatcher).status.ok());
	backend_->LoseUnsyncedWrites();
	expectBody("tags", "stale");
	AsyncStorage reopened{backend_};
	const auto latest = SysRecordsStorage{reopened, name_}.Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(latest.status.ok()) << latest.status.what();
	EXPECT_EQ(latest.content, "latest");
	backend_->LoseUnsyncedWrites();
	expectBody("tags", "latest");
	std::string value;
	EXPECT_EQ(backend_->Read(StorageOpts{}, "tags.2", value).code(), errNotFound);
}

TEST_F(AsyncStorageRecordsTest, InterruptedMigrationAfterWriteKeepsNewBodyAndRemovesSlots) {
	seedLegacy();
	backend_->FailNextWrite(RecordStorage::Failure::AfterWrite);
	const auto failed = records().Read(SysRecordKey::TagsMatcher);
	EXPECT_EQ(failed.status.code(), errLogic);
	EXPECT_STREQ(failed.status.what(), "Injected error after atomic write");
	EXPECT_TRUE(failed.content.empty());
	backend_->LoseUnsyncedWrites();
	AsyncStorage reopened{backend_};
	const auto attempts = backend_->Attempts();
	const auto latest = SysRecordsStorage{reopened, name_}.Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(latest.status.ok()) << latest.status.what();
	EXPECT_EQ(latest.content, "latest");
	EXPECT_EQ(backend_->Attempts(), attempts);
	std::string value;
	EXPECT_EQ(backend_->Read(StorageOpts{}, "tags.2", value).code(), errNotFound);
}

TEST_F(AsyncStorageRecordsTest, ReadErrorDoesNotFallBackToStaleLegacyBody) {
	seedLegacy();
	backend_->FailRead("tags.3");
	const auto attempts = backend_->Attempts();
	const auto latest = records().Read(SysRecordKey::TagsMatcher);
	EXPECT_FALSE(latest.status.ok());
	EXPECT_EQ(backend_->Attempts(), attempts);
	expectBody("tags", "stale");
}

TEST_F(AsyncStorageRecordsTest, FailedCopyCommitLeavesSourceAndItsQueueUsable) {
	records().Write("source-tags", SysRecordKey::TagsMatcher);
	storage_.Write("source-item", "original");
	{
		auto lock = storage_.LockForCopy();
		AsyncStorage copy{storage_, lock};
		SysRecordsStorage{copy, name_}.Write("copy-tags", SysRecordKey::TagsMatcher);
		copy.Write("copy-item", "uncommitted");
		backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
		EXPECT_THROW(copy.InheritUpdatesFrom(storage_, lock), Error);
		const auto attempts = backend_->Attempts();
		EXPECT_THROW(copy.InheritUpdatesFrom(storage_, lock), Error);
		EXPECT_EQ(backend_->Attempts(), attempts);
		EXPECT_EQ(backend_->ReopenAttempts(), 0);
	}
	ASSERT_TRUE(storage_.IsValid());
	storage_.Flush(StorageFlushOpts{}.WithImmediateReopen());
	EXPECT_EQ(backend_->ReopenAttempts(), 1);
	expectBody("tags", "source-tags");
	expectBody("source-item", "original");
	std::string value;
	EXPECT_EQ(backend_->Read(StorageOpts{}, "copy-item", value).code(), errNotFound);
}

TEST_F(AsyncStorageRecordsTest, CopyPersistsTagsBeforeItsItemsCanBeFlushed) {
	records().Write("source-tags", SysRecordKey::TagsMatcher);
	auto lock = storage_.LockForCopy();
	AsyncStorage copy{storage_, lock};
	SysRecordsStorage{copy, name_}.Write("copy-tags", SysRecordKey::TagsMatcher);
	copy.Write("copy-item", "committed");
	expectBody("tags", "source-tags");
	copy.InheritUpdatesFrom(storage_, lock);
	lock.unlock();
	backend_->LoseUnsyncedWrites();
	expectBody("tags", "copy-tags");
	std::string value;
	EXPECT_EQ(backend_->Read(StorageOpts{}, "copy-item", value).code(), errNotFound);
	copy.Flush(StorageFlushOpts{});
	expectBody("copy-item", "committed");
}

TEST_F(AsyncStorageRecordsTest, ReplicationStateDoesNotWaitForPendingItems) {
	storage_.Write("item", "pending");
	records().Write("state-with-item", SysRecordKey::ReplState);
	expectBody("repl", "state-with-item");
	std::string value;
	{
		auto batching = storage_.AdviceBatching();
		storage_.Flush(StorageFlushOpts{});
		EXPECT_EQ(backend_->Read(StorageOpts{}, "item", value).code(), errNotFound);
	}
	backend_->LoseUnsyncedWrites();
	expectBody("repl", "state-with-item");
	storage_.Flush(StorageFlushOpts{});
	expectBody("item", "pending");
}

TEST_F(AsyncStorageRecordsTest, CopyPreservesItemBatchOrderAndRecords) {
	for (size_t i = 0; i <= AsyncStorage::kFlushChunckSize; ++i) {
		storage_.Write("item", "source-" + std::to_string(i));
	}
	records().Write("source-state", SysRecordKey::ReplState);
	auto lock = storage_.LockForCopy();
	AsyncStorage copy{storage_, lock};
	for (size_t i = 0; i <= AsyncStorage::kFlushChunckSize; ++i) {
		copy.Write("item", "copy-" + std::to_string(i));
	}
	SysRecordsStorage{copy, name_}.Write("copy-state", SysRecordKey::ReplState);
	SysRecordsStorage{copy, name_}.Write("new-tags", SysRecordKey::TagsMatcher);
	expectBody("repl", "source-state");
	copy.InheritUpdatesFrom(storage_, lock);
	lock.unlock();
	expectBody("tags", "new-tags");
	expectBody("repl", "copy-state");
	copy.Flush(StorageFlushOpts{});
	expectBody("item", "copy-" + std::to_string(AsyncStorage::kFlushChunckSize));
	expectBody("repl", "copy-state");
}

TEST_F(AsyncStorageRecordsTest, SysRecordWriteDoesNotWaitForItemFlush) {
	storage_.Write("item", "pending");
	std::promise<void> entered, release;
	auto enteredFuture = entered.get_future();
	auto releaseFuture = release.get_future().share();
	backend_->BeforeNextWrite([&] {
		entered.set_value();
		releaseFuture.wait();
	});
	auto flushing = std::async(std::launch::async, [&] { storage_.Flush(StorageFlushOpts{}); });
	auto releaseGuard = MakeScopeGuard([&] { release.set_value(); });
	ASSERT_EQ(enteredFuture.wait_for(5s), std::future_status::ready);
	auto writing = std::async(std::launch::async, [&] { records().Write("new-tags", SysRecordKey::TagsMatcher); });
	const auto writeStatus = writing.wait_for(5s);
	release.set_value();
	releaseGuard.Disable();
	flushing.get();
	writing.get();
	EXPECT_EQ(writeStatus, std::future_status::ready);
	expectBody("item", "pending");
	backend_->LoseUnsyncedWrites();
	expectBody("tags", "new-tags");
}

TEST_F(AsyncStorageRecordsTest, SysRecordFailureDuringItemFlushBlocksFollowingBatch) {
	records().Write("old-tags", SysRecordKey::TagsMatcher);
	for (uint32_t i = 0; i < AsyncStorage::kFlushChunckSize; ++i) {
		storage_.Write("old-item", "old-value");
	}
	std::promise<void> entered, release;
	auto enteredFuture = entered.get_future();
	auto releaseFuture = release.get_future().share();
	backend_->BeforeNextWrite([&] {
		entered.set_value();
		releaseFuture.wait();
	});
	auto flushing = std::async(std::launch::async, [&] { storage_.Flush(StorageFlushOpts{}); });
	auto releaseGuard = MakeScopeGuard([&] { release.set_value(); });
	ASSERT_EQ(enteredFuture.wait_for(5s), std::future_status::ready);
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	auto writing = std::async(std::launch::async, [&] {
		Error status;
		try {
			records().Write("new-tags", SysRecordKey::TagsMatcher);
		} catch (const Error& err) {
			status = err;
		}
		storage_.Write("dependent-item", "depends-on-new-tags");
		return status;
	});
	const auto writeStatus = writing.wait_for(5s);
	release.set_value();
	releaseGuard.Disable();
	EXPECT_THROW(flushing.get(), Error);
	EXPECT_FALSE(writing.get().ok());
	EXPECT_EQ(writeStatus, std::future_status::ready);
	EXPECT_EQ(backend_->ReopenAttempts(), 0);
	expectBody("old-item", "old-value");
	expectBody("tags", "old-tags");
	std::string value;
	EXPECT_EQ(backend_->Read(StorageOpts{}, "dependent-item", value).code(), errNotFound);
	storage_.Flush(StorageFlushOpts{}.WithImmediateReopen());
	EXPECT_EQ(backend_->ReopenAttempts(), 1);
	expectBody("tags", "new-tags");
	expectBody("dependent-item", "depends-on-new-tags");
}

TEST_F(AsyncStorageRecordsTest, ConcurrentRetryCannotOverwriteNewerRecord) {
	backend_->FailNextWrite(RecordStorage::Failure::BeforeWrite);
	EXPECT_THROW(records().Write("older", SysRecordKey::TagsMatcher), Error);
	std::promise<void> entered, release;
	auto enteredFuture = entered.get_future();
	auto releaseFuture = release.get_future().share();
	backend_->BeforeNextWrite([&] {
		entered.set_value();
		releaseFuture.wait();
	});
	auto flushing = std::async(std::launch::async, [&] { storage_.Flush(StorageFlushOpts{}.WithImmediateReopen()); });
	auto releaseGuard = MakeScopeGuard([&] { release.set_value(); });
	ASSERT_EQ(enteredFuture.wait_for(5s), std::future_status::ready);
	std::promise<void> attempting;
	auto attempted = attempting.get_future();
	auto writing = std::async(std::launch::async, [&] {
		attempting.set_value();
		records().Write("newer", SysRecordKey::TagsMatcher);
	});
	attempted.wait();
	EXPECT_EQ(writing.wait_for(50ms), std::future_status::timeout);
	release.set_value();
	releaseGuard.Disable();
	flushing.get();
	writing.get();
	storage_.Flush(StorageFlushOpts{});
	backend_->LoseUnsyncedWrites();
	expectBody("tags", "newer");
}

}  // namespace
}  // namespace reindexer_tests
