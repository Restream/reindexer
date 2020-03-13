
#include "storage_lazy_load.h"
#include <mutex>
#include <thread>
#include "estl/shared_mutex.h"

void waitFor(int millisec) { std::this_thread::sleep_for(std::chrono::milliseconds(millisec)); }

TEST_F(DISABLED_StorageLazyLoadApi, BasicTest) {
	bool storageLoaded = false;
	int64_t totalItemsCount = getItemsCount(storageLoaded);

	waitFor(2500);
	int64_t itemsNow = getItemsCount(storageLoaded);
	EXPECT_TRUE(itemsNow == 0) << "Expected 0 items, not " << itemsNow;
	EXPECT_TRUE(!storageLoaded);

	SelectAll();
	itemsNow = getItemsCount(storageLoaded);
	totalItemsCount += 100;	 // inserted 100 + 100 from storage (inserted before close of NS)
	EXPECT_TRUE(itemsNow == totalItemsCount) << "Expected " << totalItemsCount << " items, not " << itemsNow;
	EXPECT_TRUE(storageLoaded);

	fillNs(100);
	totalItemsCount += 100;

	SelectAll();
	itemsNow = getItemsCount(storageLoaded);
	EXPECT_TRUE(itemsNow == totalItemsCount) << "Expected " << totalItemsCount << " items after insertion, not " << itemsNow;
	EXPECT_TRUE(storageLoaded);

	waitFor(2500);
	itemsNow = getItemsCount(storageLoaded);
	EXPECT_TRUE(itemsNow == 0) << "Expected 0 items, not " << itemsNow;
	EXPECT_TRUE(!storageLoaded);
}

TEST_F(DISABLED_StorageLazyLoadApi, ReadWriteTest) {
	bool storageLoaded = false;
	int totalItemsCount(getItemsCount(storageLoaded));
	totalItemsCount += 100;	 // 100 is not loaded from storage yet

	enum LastOperation : int { Select, Insert };
	int lastOperation;
	std::mutex m;

	waitFor(2500);

	std::vector<std::thread> selectThreads;
	std::vector<std::thread> insertThreads;
	for (size_t i = 0; i < 5; ++i) {
		selectThreads.emplace_back([&]() {
			std::lock_guard<std::mutex> lk(m);
			SelectAll();
			int items = getItemsCount(storageLoaded);
			EXPECT_TRUE(items == totalItemsCount) << "Expected to select " << totalItemsCount << " items, but selected " << items;
			EXPECT_TRUE(storageLoaded);
			lastOperation = LastOperation::Select;
		});
		insertThreads.emplace_back([&]() {
			std::lock_guard<std::mutex> lk(m);
			fillNs(100);
			totalItemsCount += 100;

			int items = getItemsCount(storageLoaded);
			if ((lastOperation == LastOperation::Select) && storageLoaded) {
				EXPECT_TRUE(totalItemsCount == items);
			} else if (!storageLoaded) {
				EXPECT_TRUE(items >= 0);
			}
			lastOperation = LastOperation::Insert;
		});
	}

	for (size_t i = 0; i < 5; ++i) {
		selectThreads[i].join();
		insertThreads[i].join();
	}
}

TEST_F(DISABLED_StorageLazyLoadApi, DISABLED_AttemptToReadWriteInParallelTest) {
	bool storageLoaded = false;
	std::atomic<int> totalItemsCount(getItemsCount(storageLoaded));
	totalItemsCount += 100;	 // 100 is not loaded from storage yet

	reindexer::shared_timed_mutex mtx;

	auto writeFn = [&]() {
		mtx.lock_shared();
		fillNs(20);
		mtx.unlock_shared();
		totalItemsCount += 20;
	};

	auto readFn = [&]() {
		mtx.lock();
		SelectAll();
		bool loaded = false;
		int items = getItemsCount(loaded);
		EXPECT_TRUE(loaded) << "Storage was reloaded";
		EXPECT_TRUE(items == inserted_) << "Expected " << inserted_ << ", got " << items << std::endl;
		mtx.unlock();
		waitFor(0);
	};

	// here we reload it (i.e. make items count = 0)
	// and after first Select it's size should include
	// items from storage
	waitFor(4000);

	std::vector<std::thread> selectThreads;
	for (size_t i = 0; i < 5; ++i) {
		selectThreads.push_back(std::thread(readFn));
	}

	std::vector<std::thread> insertThreads;
	for (size_t i = 0; i < 5; ++i) {
		insertThreads.push_back(std::thread(writeFn));
	}

	for (size_t i = 0; i < 5; ++i) {
		selectThreads.push_back(std::thread(readFn));
	}

	for (size_t i = 0; i < selectThreads.size(); ++i) {
		selectThreads[i].join();
	}

	for (size_t i = 0; i < insertThreads.size(); ++i) {
		insertThreads[i].join();
	}

	SelectAll();
	EXPECT_TRUE(totalItemsCount == inserted_);
}

TEST_F(DISABLED_StorageLazyLoadApi, TestForTSAN) {
	auto writeFn = [&]() { fillNs(200); };
	auto readFn = [&]() {
		SelectAll();
		waitFor(0);
	};

	// here we reload it (i.e. make items count = 0)
	// and after first Select it's size should include
	// items from storage
	waitFor(2500);

	std::vector<std::thread> selectThreads;
	for (size_t i = 0; i < 3; ++i) {
		selectThreads.push_back(std::thread(readFn));
	}

	std::vector<std::thread> insertThreads;
	for (size_t i = 0; i < 15; ++i) {
		insertThreads.push_back(std::thread(writeFn));
	}

	for (size_t i = 0; i < 2; ++i) {
		selectThreads.push_back(std::thread(readFn));
	}

	for (size_t i = 0; i < 5; ++i) {
		insertThreads.push_back(std::thread(writeFn));
	}

	for (size_t i = 0; i < 5; ++i) {
		selectThreads.push_back(std::thread(readFn));
	}

	for (size_t i = 0; i < selectThreads.size(); ++i) {
		selectThreads[i].join();
	}

	for (size_t i = 0; i < insertThreads.size(); ++i) {
		insertThreads[i].join();
	}
}
