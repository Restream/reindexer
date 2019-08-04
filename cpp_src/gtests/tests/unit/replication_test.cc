#include <unordered_map>
#include <unordered_set>
#include "replication_load_api.h"

TEST_F(ReplicationLoadApi, Base) {
	InitNs();
	stop = false;
	FillData(1000);

	std::thread destroyer([this]() {
		while (!stop) {
			int i = rand() % 4;
			RestartServer(i, i);
			std::this_thread::sleep_for(std::chrono::seconds(3));
		}
	});

	for (size_t i = 0; i < 2; ++i) {
		if (i % 3 == 0) DeleteFromMaster();
		FillData(1000);
		SimpleSelect(0);
	}

	stop = true;
	destroyer.join();
}

TEST_F(ReplicationLoadApi, DISABLED_BasicTestNoMasterRestart) {
	InitNs();
	stop = false;
	FillData(1000);

	std::thread destroyer([this]() {
		while (!stop) {
			RestartServer(rand() % 3 + 1, true);
			std::this_thread::sleep_for(std::chrono::seconds(3));
		}
	});

	for (size_t i = 0; i < 10; ++i) {
		if (i % 3 == 0) DeleteFromMaster();
		FillData(1000);
		SimpleSelect(0);
		SimpleSelect(rand() % 3 + 1);
	}

	stop = true;
	destroyer.join();
}

TEST_F(ReplicationLoadApi, SingleSlaveTest) {
	InitNs();
	stop = false;
	FillData(1000);

	std::thread writingThread([this]() {
		while (!stop) {
			FillData(1000);
		}
	});

	std::thread removingThread([this]() {
		size_t counter = 0;
		while (!stop) {
			std::this_thread::sleep_for(std::chrono::seconds(3));
			int i = rand() % 2;
			counter++;

			RestartServer(i, i);
			if (counter % 3 == 0) DeleteFromMaster();
		}
	});

	for (size_t i = 0; i < 2; ++i) {
		SimpleSelect(0);
		std::this_thread::sleep_for(std::chrono::seconds(3));
	}

	stop = true;
	writingThread.join();
	removingThread.join();
}
