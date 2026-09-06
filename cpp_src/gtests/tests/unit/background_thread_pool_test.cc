#include "tools/background_thread_pool.h"
#include "gtest/gtest.h"

namespace reindexer_tests {

TEST(BackgroundThreadPool, ReturnsSingleProcessWideInstance) {
	auto& first = reindexer::GetBackgroundThreadPool();
	auto& second = reindexer::GetBackgroundThreadPool();

	EXPECT_EQ(&first, &second);
	EXPECT_GT(first.get_thread_count(), 0);
}

}  // namespace reindexer_tests
