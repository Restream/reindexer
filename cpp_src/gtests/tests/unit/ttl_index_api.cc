#include "ttl_index_api.h"
#include <vector>

TEST_F(TtlIndexApi, ItemsSimpleVanishing) {
	std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	size_t count = WaitForVanishing();
	ASSERT_EQ(count, 0);

	AddDataToNs(5000);

	std::this_thread::sleep_for(std::chrono::milliseconds(2000));

	count = WaitForVanishing();
	ASSERT_EQ(count, 0);
}

TEST_F(TtlIndexApi, ItemsVanishingAfterInsertRemove) {
	std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	size_t count = WaitForVanishing();
	ASSERT_EQ(count, 0);
	AddDataToNs(3000);

	std::vector<std::thread> threads;
	threads.reserve(3);

	threads.emplace_back(std::thread(&TtlIndexApi::InsertItemsSlowly, this));
	threads.emplace_back(std::thread(&TtlIndexApi::RemoveItemsSlowly, this));
	threads.emplace_back(std::thread(&TtlIndexApi::SelectData, this));

	for (size_t i = 0; i < threads.size(); ++i) {
		threads[i].join();
	}

	std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	count = WaitForVanishing();
	ASSERT_EQ(count, 0);
}
