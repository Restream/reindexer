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

TEST_F(ReplicationLoadApi, ConfigSync) {
	ServerConfig config("slave", true, false, "cproto://127.0.0.1:6534/0");
	RestartWithConfigFile(2,
						  "role: slave\n"
						  "master_dsn: cproto://127.0.0.1:6534/0\n"
						  "cluster_id: 2\n"
						  "force_sync_on_logic_error: true\n"
						  "force_sync_on_wrong_data_hash: false\n"
						  "namespaces: []");
	CheckSlaveConfigFile(2, config);
	config = ServerConfig("slave", false, true, "cproto://127.0.0.1:6534/12345", {"ns1", "ns2"});
	SetServerConfig(2, config);
	CheckSlaveConfigFile(2, config);
	config = ServerConfig("slave", true, false, "cproto://127.0.0.1:6534/999");
	SetServerConfig(2, config);
	CheckSlaveConfigFile(2, config);

	GetSrv(2)->WriteServerConfig(
		"role: slave\n"
		"master_dsn: cproto://127.0.0.1:6534/somensname\n"
		"cluster_id: 2\n"
		"force_sync_on_logic_error: false\n"
		"force_sync_on_wrong_data_hash: true\n"
		"namespaces:\n"
		"  - ns1\n"
		"  - ns3\n");
	config = ServerConfig("slave", false, true, "cproto://127.0.0.1:6534/somensname", {"ns1", "ns3"});
	std::this_thread::sleep_for(std::chrono::seconds(3));
	CheckSlaveConfigNamespace(2, config);
}
