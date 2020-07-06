#include <unordered_map>
#include <unordered_set>
#include "replication_load_api.h"

TEST_F(ReplicationLoadApi, Base) {
	InitNs();
	stop = false;
	FillData(1000);

	std::thread destroyer([this]() {
		int count = 0;
		while (!stop) {
			if (!(count % 30)) {
				int i = rand() % kDefaultServerCount;
				RestartServer(i);
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	});

	for (size_t i = 0; i < 2; ++i) {
		if (i % 3 == 0) DeleteFromMaster();
		FillData(1000);
		SimpleSelect(0);
	}

	stop = true;
	destroyer.join();

	ForceSync();
	WaitSync("some");
	WaitSync("some1");
}

TEST_F(ReplicationLoadApi, DISABLED_BasicTestNoMasterRestart) {
	InitNs();
	stop = false;
	FillData(1000);

	std::thread destroyer([this]() {
		while (!stop) {
			RestartServer(rand() % 3 + 1);
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

			RestartServer(i);
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
	ForceSync();
	WaitSync("some");
	WaitSync("some1");
}

TEST_F(ReplicationLoadApi, ConfigSync) {
	ReplicationConfigTest config("slave", true, false, 0, "cproto://127.0.0.1:6534/0", "slave_1");
	const size_t kTestSlaveID = 2;
	RestartWithConfigFile(kTestSlaveID,
						  "role: slave\n"
						  "master_dsn: cproto://127.0.0.1:6534/0\n"
						  "app_name: slave_1\n"
						  "cluster_id: 2\n"
						  "force_sync_on_logic_error: true\n"
						  "force_sync_on_wrong_data_hash: false\n"
						  "namespaces: []");
	CheckSlaveConfigFile(kTestSlaveID, config);
	config = ReplicationConfigTest("slave", false, true, 0, "cproto://127.0.0.1:6534/12345", "slave_1", {"ns1", "ns2"});
	SetServerConfig(kTestSlaveID, config);
	CheckSlaveConfigFile(kTestSlaveID, config);
	config = ReplicationConfigTest("slave", true, false, 0, "cproto://127.0.0.1:6534/999", "slave_1");
	SetServerConfig(kTestSlaveID, config);
	CheckSlaveConfigFile(kTestSlaveID, config);
	std::this_thread::sleep_for(std::chrono::seconds(2));  // In case if OS doesn't have nanosecods in stat result

	GetSrv(kTestSlaveID)
		->WriteServerConfig(
			"role: slave\n"
			"master_dsn: cproto://127.0.0.1:6534/somensname\n"
			"app_name: slave_1\n"
			"cluster_id: 2\n"
			"force_sync_on_logic_error: false\n"
			"force_sync_on_wrong_data_hash: true\n"
			"namespaces:\n"
			"  - ns1\n"
			"  - ns3\n");
	config = ReplicationConfigTest("slave", false, true, 0, "cproto://127.0.0.1:6534/somensname", "slave_1", {"ns1", "ns3"});
	CheckSlaveConfigNamespace(kTestSlaveID, config, std::chrono::seconds(3));
}

TEST_F(ReplicationLoadApi, DynamicRoleSwitch) {
	InitNs();
	for (size_t i = 1; i < 8; i++) {
		FillData(1000);
		WaitSync("some");
		WaitSync("some1");
		SwitchMaster(i % 4);
	}
}
