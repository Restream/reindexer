#pragma once

#include "reindexer_api.h"
#include "tools/fsops.h"

namespace reindexer_tests {

class [[nodiscard]] ReindexerStorageApi : public ReindexerApi {
protected:
	void SetUp() override {
		ReindexerApi::SetUp();

		const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
		storagePath_ =
			reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), std::string("reindex/") + info->test_suite_name() + "_" + info->name());
		rt.reindexer.reset();
		std::ignore = reindexer::fs::RmDirAll(storagePath_);
		rt.reindexer = std::make_shared<Reindexer>();
		rt.Connect("builtin://" + storagePath_);
		rt.OpenNamespace(default_namespace, StorageOpts().Enabled().CreateIfMissing());
	}

	void TearDown() override {
		rt.reindexer.reset();
		std::ignore = reindexer::fs::RmDirAll(storagePath_);
		ReindexerApi::TearDown();
	}

	void ReopenNamespace() {
		rt.CloseNamespace(default_namespace);
		rt.OpenNamespace(default_namespace, StorageOpts().Enabled().CreateIfMissing());
	}

	void PutMeta(const std::string& key, std::string_view data) {
		auto err = rt.reindexer->PutMeta(default_namespace, key, data);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	std::string GetMeta(const std::string& key) {
		std::string data;
		auto err = rt.reindexer->GetMeta(default_namespace, key, data);
		EXPECT_TRUE(err.ok()) << err.what();
		return data;
	}

	std::vector<reindexer::ShardedMeta> GetShardedMeta(const std::string& key) {
		std::vector<reindexer::ShardedMeta> data;
		auto err = rt.reindexer->GetMeta(default_namespace, key, data);
		EXPECT_TRUE(err.ok()) << err.what();
		return data;
	}

	void DeleteMeta(const std::string& key) {
		auto err = rt.reindexer->DeleteMeta(default_namespace, key);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	std::vector<std::string> EnumMeta() {
		std::vector<std::string> keys;
		auto err = rt.reindexer->EnumMeta(default_namespace, keys);
		EXPECT_TRUE(err.ok()) << err.what();
		return keys;
	}

private:
	std::string storagePath_;
};

}  // namespace reindexer_tests
