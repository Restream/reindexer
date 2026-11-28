#include "core/namespace/sys_records_storage.h"
#include <array>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include "core/namespace/asyncstorage.h"
#include "core/namespace/namespacename.h"
#include "gtest/gtest.h"
#include "tools/fsops.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer_tests {
namespace {

using namespace reindexer;
using SysRecordKey = SysRecordsStorage::Key;

static_assert(noexcept(std::declval<const SysRecordsStorage&>().Read(SysRecordKey::TagsMatcher)));

class [[nodiscard]] SysRecordsStorageTest : public ::testing::Test {
protected:
	void SetUp() override {
		path_ = fs::JoinPath(fs::GetTempDir(),
							 std::string{"SysRecordsStorageTest."} + ::testing::UnitTest::GetInstance()->current_test_info()->name());
		std::ignore = fs::RmDirAll(path_);
		const auto err = storage_.Open(datastorage::StorageType::LevelDB, name_, path_, StorageOpts{}.CreateIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
	}
	void TearDown() override {
		storage_.Close();
		std::ignore = fs::RmDirAll(path_);
	}
	SysRecordsStorage records() { return SysRecordsStorage{storage_, name_}; }
	void expectBody(std::string_view key, std::string_view expectedBody) {
		std::string data;
		const auto err = storage_.Read(StorageOpts{}, key, data);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(data, expectedBody);
	}
	void expectNoSlots(std::string_view keyName = kStorageKey) {
		for (size_t i = 0; i < 8; ++i) {
			std::string data;
			EXPECT_EQ(storage_.Read(StorageOpts{}, std::string{keyName} + "." + std::to_string(i), data).code(), errNotFound);
		}
	}
	void seedSlot(size_t slot, uint64_t version, std::string_view body) {
		WrSerializer ser;
		ser.PutUInt64(version);
		ser.Write(body);
		storage_.WriteSync(StorageOpts{}, std::string{kStorageKey} + "." + std::to_string(slot), ser.Slice());
	}
	static constexpr std::string_view kStorageKey = "tags";
	NamespaceName name_{"sys_records_test"};
	AsyncStorage storage_;
	std::string path_;
};

TEST_F(SysRecordsStorageTest, SingleBodySurvivesReopenWithoutVersionHeader) {
	constexpr std::string_view kBody{"body\0tail", 9};
	records().Write("first", SysRecordKey::TagsMatcher);
	records().Write(kBody, SysRecordKey::TagsMatcher);
	expectBody(kStorageKey, kBody);
	expectNoSlots();
	storage_.Close();
	const auto err = storage_.Open(datastorage::StorageType::LevelDB, name_, path_, StorageOpts{});
	ASSERT_TRUE(err.ok()) << err.what();
	const auto latest = records().Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(latest.status.ok()) << latest.status.what();
	EXPECT_EQ(latest.content, kBody);
}

TEST_F(SysRecordsStorageTest, MigratesNewestSlotOverStaleUnsuffixedBody) {
	storage_.WriteSync(StorageOpts{}, kStorageKey, "stale-legacy-body");
	for (size_t i = 0; i < 8; ++i) {
		seedSlot(i, i < 3 ? i + 8 : i, "body-" + std::to_string(i));
	}
	const auto migrated = records().Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(migrated.status.ok()) << migrated.status.what();
	EXPECT_EQ(migrated.content, "body-2");
	expectBody(kStorageKey, "body-2");
	expectNoSlots();
	records().Write("new-body", SysRecordKey::TagsMatcher);
	const auto latest = records().Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(latest.status.ok()) << latest.status.what();
	EXPECT_EQ(latest.content, "new-body");
	expectNoSlots();
}

TEST_F(SysRecordsStorageTest, MigratesSlotsRecreatedByAnOlderWriter) {
	seedSlot(0, 1, "before-upgrade");
	const auto initial = records().Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(initial.status.ok()) << initial.status.what();
	records().Write("after-upgrade", SysRecordKey::TagsMatcher);
	// An older binary reads the unsuffixed body, then starts writing rotating copies again.
	seedSlot(3, 3, "after-downgrade");
	const auto migrated = records().Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(migrated.status.ok()) << migrated.status.what();
	EXPECT_EQ(migrated.content, "after-downgrade");
	expectBody(kStorageKey, "after-downgrade");
	expectNoSlots();
}

TEST_F(SysRecordsStorageTest, LegacyUnsuffixedBinaryBodiesRemainReadableAndWritable) {
	const std::array<std::pair<SysRecordKey, std::string_view>, 5> keys{{
		{SysRecordKey::Indexes, "indexes"},
		{SysRecordKey::Schema, "schema"},
		{SysRecordKey::ReplState, "repl"},
		{SysRecordKey::TagsMatcher, "tags"},
		{SysRecordKey::PkMigrationStatus, "pk_migration_status"},
	}};
	constexpr std::string_view kBody{"old\0body", 8};
	for (const auto& [key, keyName] : keys) {
		SCOPED_TRACE(keyName);
		storage_.WriteSync(StorageOpts{}, keyName, kBody);
		const auto legacy = records().Read(key);
		ASSERT_TRUE(legacy.status.ok()) << legacy.status.what();
		EXPECT_EQ(legacy.content, kBody);
		records().Write("updated", key);
		expectBody(keyName, "updated");
		expectNoSlots(keyName);
	}
}

TEST_F(SysRecordsStorageTest, EmptyVersionedBodyTakesPrecedenceOverLegacyRecord) {
	storage_.WriteSync(StorageOpts{}, kStorageKey, "legacy-body");
	seedSlot(0, 1, {});
	const auto latest = records().Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(latest.status.ok()) << latest.status.what();
	EXPECT_TRUE(latest.content.empty());
	expectBody(kStorageKey, {});
	expectNoSlots();
	const auto again = records().Read(SysRecordKey::TagsMatcher);
	EXPECT_TRUE(again.status.ok());
	EXPECT_TRUE(again.content.empty());
}

TEST_F(SysRecordsStorageTest, RelatedRecordsAreWrittenTogether) {
	const std::array updates{SysRecordsStorage::Record{.key = SysRecordKey::Indexes, .value = "new-indexes"},
							 SysRecordsStorage::Record{.key = SysRecordKey::TagsMatcher, .value = "new-tags"}};
	records().Write(updates);
	expectBody("indexes", "new-indexes");
	expectBody("tags", "new-tags");
}

TEST_F(SysRecordsStorageTest, MissingRecordAndMalformedHeaderAreDistinct) {
	const auto missing = records().Read(SysRecordKey::TagsMatcher);
	EXPECT_EQ(missing.status.code(), errNotFound);
	storage_.WriteSync(StorageOpts{}, kStorageKey, "legacy-body");
	storage_.WriteSync(StorageOpts{}, "tags.0", "x");
	const auto malformed = records().Read(SysRecordKey::TagsMatcher);
	EXPECT_EQ(malformed.status.code(), errParseBin);
	EXPECT_STREQ(malformed.status.what(), "Binary buffer underflow. Need more 7 bytes, pos=0,len=1");
	EXPECT_TRUE(malformed.content.empty());
	expectBody(kStorageKey, "legacy-body");
	expectBody("tags.0", "x");
}

TEST_F(SysRecordsStorageTest, AbandonedCopyDoesNotChangeStorage) {
	records().Write("original", SysRecordKey::TagsMatcher);
	{
		auto lock = storage_.LockForCopy();
		AsyncStorage copy{storage_, lock};
		SysRecordsStorage{copy, name_}.Write("uncommitted", SysRecordKey::TagsMatcher);
		copy.Flush(StorageFlushOpts{});
	}
	expectBody(kStorageKey, "original");
}

TEST_F(SysRecordsStorageTest, CopyCommitsRecordsBeforeQueuedItems) {
	records().Write("original", SysRecordKey::TagsMatcher);
	auto lock = storage_.LockForCopy();
	AsyncStorage copy{storage_, lock};
	SysRecordsStorage{copy, name_}.Write("committed", SysRecordKey::TagsMatcher);
	copy.Write("item", "uses-new-tags");
	copy.InheritUpdatesFrom(storage_, lock);
	lock.unlock();
	const auto latest = SysRecordsStorage{copy, name_}.Read(SysRecordKey::TagsMatcher);
	ASSERT_TRUE(latest.status.ok()) << latest.status.what();
	EXPECT_EQ(latest.content, "committed");
	std::string data;
	EXPECT_EQ(copy.Read(StorageOpts{}, "item", data).code(), errNotFound);
	copy.Flush(StorageFlushOpts{});
	const auto err = copy.Read(StorageOpts{}, "item", data);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(data, "uses-new-tags");
	copy.Close();
}

}  // namespace
}  // namespace reindexer_tests
