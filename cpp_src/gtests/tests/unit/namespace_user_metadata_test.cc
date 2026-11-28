#include "core/namespace/namespace_user_metadata.h"

#include <algorithm>
#include <string>
#include <tuple>
#include <vector>
#include "core/namespace/asyncstorage.h"
#include "core/type_consts.h"
#include "gtest/gtest.h"
#include "tools/fsops.h"

namespace reindexer_tests {
namespace {

class [[nodiscard]] NamespaceUserMetadataTest : public ::testing::Test {
protected:
	void SetUp() override {
		path_ = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), std::string{"NamespaceUserMetadataTest."} +
																		 ::testing::UnitTest::GetInstance()->current_test_info()->name());
		std::ignore = reindexer::fs::RmDirAll(path_);
		const auto err =
			storage_.Open(reindexer::datastorage::StorageType::LevelDB, "metadata_reload", path_, StorageOpts{}.CreateIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
	}
	void TearDown() override {
		storage_.Close();
		std::ignore = reindexer::fs::RmDirAll(path_);
	}

	reindexer::AsyncStorage storage_;

private:
	std::string path_;
};

TEST_F(NamespaceUserMetadataTest, ReloadReplacesExistingValuesAndRemovesMissingKeys) {
	reindexer::NamespaceUserMetadata writer;
	reindexer::NamespaceUserMetadata loaded;
	writer.Put("changed", "old", storage_);
	writer.Put("removed", "value", storage_);
	storage_.Flush(reindexer::StorageFlushOpts{});
	loaded.LoadFromStorage(storage_);
	ASSERT_EQ(loaded.Get("changed"), "old");
	ASSERT_EQ(loaded.Get("removed"), "value");
	ASSERT_EQ(loaded.EnumKeys().size(), 2);

	writer.Put("changed", "new", storage_);
	writer.Remove("removed", storage_);
	writer.Put("added", "value", storage_);
	storage_.Flush(reindexer::StorageFlushOpts{});
	loaded.LoadFromStorage(storage_);
	EXPECT_EQ(loaded.Get("changed"), "new");
	EXPECT_EQ(loaded.Get("removed"), "");
	EXPECT_EQ(loaded.Get("added"), "value");
	auto keys = loaded.EnumKeys();
	std::sort(keys.begin(), keys.end());
	EXPECT_EQ(keys, (std::vector<std::string>{"added", "changed"}));

	writer.Remove("changed", storage_);
	writer.Remove("added", storage_);
	storage_.Flush(reindexer::StorageFlushOpts{});
	loaded.LoadFromStorage(storage_);
	EXPECT_TRUE(loaded.EnumKeys().empty());
}

}  // namespace
}  // namespace reindexer_tests
