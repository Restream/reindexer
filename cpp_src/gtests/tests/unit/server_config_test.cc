#include "gtest/gtest.h"
#include "server/config.h"
#include "tools/fsops.h"

#include <filesystem>
#include <string_view>

namespace reindexer_tests {

TEST(ServerConfig, ParseFileRejectsDirectory) {
	const auto configDir{reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindexer_server_config_parse_file_test")};
	std::error_code ec;
	std::filesystem::remove_all(configDir, ec);
	ec.clear();

	ASSERT_TRUE(std::filesystem::create_directories(configDir, ec)) << ec.message();

	reindexer_server::ServerConfig config{false};
	const auto err = config.ParseFile(configDir);

	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_NE(std::string_view{err.what()}.find("not a regular file"), std::string_view::npos) << err.what();

	ec.clear();
	std::filesystem::remove_all(configDir, ec);
}

}  // namespace reindexer_tests
