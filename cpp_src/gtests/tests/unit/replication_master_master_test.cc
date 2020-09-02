#include <gtest/gtest.h>
#include <cstdio>
#include <thread>
#include "client/reindexer.h"
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "core/keyvalue/p_string.h"
#include "server/server.h"
#include "servercontrol.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"

using namespace reindexer;

class ReplicationSlaveSlaveApi : public ::testing::Test {
protected:
	void SetUp() {}

	void TearDown() {}

public:
	ReplicationSlaveSlaveApi() {}
	void WaitSync(ServerControl& s1, ServerControl& s2, const std::string& nsName) {
		int count = 0;
		ReplicationStateApi state1, state2;
		while (count < 1000) {
			state1 = s1.Get()->GetState(nsName);
			state2 = s2.Get()->GetState(nsName);
			if (state1.lsn == state2.lsn) {
				return;
			}

			count++;
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		EXPECT_TRUE(false) << "Wait sync is too long. s1 lsn: " << state1.lsn << "; s2 lsn: " << state2.lsn;
	}

	void CreateConfiguration(vector<ServerControl>& nodes, const std::vector<int>& slaveConfiguration, int basePort, int baseServerId,
							 const std::string& dbPathMaster) {
		for (size_t i = 0; i < slaveConfiguration.size(); i++) {
			nodes.push_back(ServerControl());
			nodes.back().InitServer(i, basePort + i, basePort + 1000 + i, dbPathMaster + std::to_string(i), "db");
			if (i == 0) {
				ReplicationConfigTest config("master");
				nodes.back().Get()->MakeMaster(config);
			} else {
				std::string masterDsn = "cproto://127.0.0.1:" + std::to_string(slaveConfiguration[i]) + "/db";
				ReplicationConfigTest config("slave", false, true, baseServerId + i, masterDsn);
				nodes.back().Get()->MakeSlave(0, config);
			}
		}
	}

	void RestartServer(size_t id, vector<ServerControl>& nodes, int port, const std::string& dbPathMaster) {
		assert(id < nodes.size());
		if (nodes[id].Get()) {
			nodes[id].Drop();
			size_t counter = 0;
			while (nodes[id].IsRunning()) {
				counter++;
				// we have only 10sec timeout to restart server!!!!
				EXPECT_TRUE(counter < 1000);
				assert(counter < 1000);

				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
		}
		nodes[id].InitServer(id, port + id, port + 1000 + id, dbPathMaster + std::to_string(id), "db");
	}

private:
};

class TestNamespace1 {
public:
	TestNamespace1(ServerControl& masterControl, const std::string nsName = std::string("ns1")) : nsName_(nsName) {
		auto master = masterControl.Get();
		auto opt = StorageOpts().Enabled(true);
		Error err = master->api.reindexer->OpenNamespace(nsName_, opt);
		master->api.DefineNamespaceDataset(nsName_, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
	}

	void AddRows(ServerControl& masterControl, int from, unsigned int count) {
		auto master = masterControl.Get();
		for (unsigned int i = 0; i < count; i++) {
			reindexer::client::Item item = master->api.NewItem(nsName_);
			auto err = item.FromJSON("{\"id\":" + std::to_string(from + i) + "}");
			ASSERT_TRUE(err.ok()) << err.what();
			master->api.Upsert(nsName_, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	std::function<void(ServerControl&, int, unsigned int, std::string)> AddRow1msSleep = [](ServerControl& masterControl, int from,
																							unsigned int count, std::string nsName) {
		auto master = masterControl.Get();
		for (unsigned int i = 0; i < count; i++) {
			reindexer::client::Item item = master->api.NewItem("ns1");
			auto err = item.FromJSON("{\"id\":" + std::to_string(i + from) + "}");
			ASSERT_TRUE(err.ok()) << err.what();
			master->api.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	};

	void GetData(ServerControl& node, std::vector<int>& ids) {
		Query qr = Query(nsName_).Sort("id", false);
		client::QueryResults res;
		auto err = node.Get()->api.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();
		for (auto it : res) {
			WrSerializer ser;
			auto err = it.GetJSON(ser, false);
			EXPECT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			auto root = parser.Parse(ser.Slice());
			ids.push_back(root["id"].As<int>());
		}
	}
	const std::string nsName_;
};

TEST_F(ReplicationSlaveSlaveApi, MasterSlaveSyncByWalAddRow) {
	std::string dbPathMaster("/tmp/test_slave/test_");
	reindexer::fs::RmDirAll("/tmp/test_slave/");

	const int port = 9999;

	std::vector<int> slaveConfiguration = {-1, port};
	vector<ServerControl> nodes;
	CreateConfiguration(nodes, slaveConfiguration, port, 10, dbPathMaster);

	TestNamespace1 ns1(nodes[0]);

	nodes[0].Get()->SetWALSize(100000, "ns1");
	int n1 = 1000;
	ns1.AddRows(nodes[0], 0, n1);

	//синхронизируемся
	WaitSync(nodes[0], nodes[1], "ns1");
	//выключаем Slave
	if (nodes[1].Get()) {
		nodes[1].Drop();
		size_t counter = 0;
		while (nodes[1].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter < 1000);
			assert(counter < 1000);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}

	const int startId = 10000;
	const unsigned int n2 = 2000;
	ServerControl& master = nodes[0];
	auto ThreadAdd = [&master, &ns1]() {
		master.Get()->SetWALSize(50000, "ns1");
		ns1.AddRow1msSleep(master, startId, n2, ns1.nsName_);
	};

	std::thread insertThread(ThreadAdd);

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	nodes[1].InitServer(1, port + 1, port + 1000 + 1, dbPathMaster + std::to_string(1), "db");

	insertThread.join();

	WaitSync(nodes[0], nodes[1], "ns1");

	std::vector<int> ids0;
	ns1.GetData(nodes[0], ids0);
	std::vector<int> ids1;
	ns1.GetData(nodes[1], ids1);

	EXPECT_TRUE(ids1.size() == (n1 + n2));
	EXPECT_TRUE(ids0 == ids1);
}

TEST_F(ReplicationSlaveSlaveApi, MasterSlaveStart) {
	std::string dbPathMaster("/tmp/test_slave/test_");
	reindexer::fs::RmDirAll("/tmp/test_slave/");
	const int port = 9999;

	std::vector<int> slaveConfiguration = {-1, port};
	vector<ServerControl> nodes;
	CreateConfiguration(nodes, slaveConfiguration, port, 10, dbPathMaster);

	//вставляем 100 строк
	std::string nsName("ns1");
	TestNamespace1 ns1(nodes[0], nsName);

	unsigned int n1 = 100;
	ns1.AddRows(nodes[0], 0, n1);
	nodes[0].Get()->SetWALSize(1000, "ns1");
	//синхронизируемся

	WaitSync(nodes[0], nodes[1], nsName);
	// restart Slave
	RestartServer(1, nodes, port, dbPathMaster);
	//проверяем, что они синхронны
	WaitSync(nodes[0], nodes[1], nsName);

	//останавливаем Slave
	//выключаем Slave
	if (nodes[1].Get()) {
		nodes[1].Drop();
		size_t counter = 0;
		while (nodes[1].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter < 1000);
			assert(counter < 1000);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	//вставляем 100 строк (должно быть в сумме 200)
	ns1.AddRows(nodes[0], n1 + 1, n1);

	//запускаем Slave
	nodes[1].InitServer(1, port + 1, port + 1000 + 1, dbPathMaster + std::to_string(1), "db");
	//проверяем, что они синхронны
	WaitSync(nodes[0], nodes[1], nsName);

	std::vector<int> ids0;
	ns1.GetData(nodes[0], ids0);
	std::vector<int> ids1;
	ns1.GetData(nodes[1], ids1);

	EXPECT_TRUE(ids1.size() == (n1 + n1));
	EXPECT_TRUE(ids0 == ids1);
}

TEST_F(ReplicationSlaveSlaveApi, MasterSlaveSlave2) {
	auto SimpleTest = [this](int port, const std::vector<int>& slaveConfiguration) {
		std::string dbPathMaster("/tmp/test_slave/test_");
		reindexer::fs::RmDirAll("/tmp/test_slave/");
		int serverId = 5;
		vector<ServerControl> nodes;

		CreateConfiguration(nodes, slaveConfiguration, port, serverId, dbPathMaster);

		ServerControl& master = nodes[0];
		TestNamespace1 ns1(master);

		const int count = 1000;
		ns1.AddRows(master, 0, count);

		for (size_t i = 1; i < nodes.size(); i++) {
			WaitSync(nodes[0], nodes[i], "ns1");
		}

		std::vector<std::vector<int>> results;

		Query qr = Query("ns1").Sort("id", true);
		for (size_t i = 0; i < nodes.size(); i++) {
			results.push_back(vector<int>());
			ns1.GetData(nodes[i], results.back());
		}

		for (size_t i = 1; i < results.size(); ++i) {
			EXPECT_TRUE((results[0] == results[i]));
		}
	};

	const int port = 9999;
	{
		/*
				m
				|
				1
				|
				2
		*/
		std::vector<int> slaveConfiguration = {-1, port, port + 1};
		SimpleTest(port, slaveConfiguration);
	}
	{
		/*
				m
			   / \
			  1   2
			  |   | \
			  3   4  5
		*/

		std::vector<int> slaveConfiguration = {-1, port, port, port + 1, port + 2, port + 2};
		SimpleTest(port, slaveConfiguration);
	}
}

//--

//--
#if !defined(REINDEX_WITH_TSAN)

TEST_F(ReplicationSlaveSlaveApi, MasterSlaveSlaveReload) {
	const int port = 9999;
	const std::string dbPathMaster = "/tmp/test_slave2reload/test_";
	reindexer::fs::RmDirAll("/tmp/test_slave2reload/");
	const int serverId = 5;
	vector<ServerControl> nodes;
	std::atomic_bool stopRestartServerThread(false);

	/*
			m
		   / \
		  1   2
		  |   | \
		  3   4  5
	*/
	std::vector<int> slaveConfiguration = {-1, port, port, port + 1, port + 2, port + 2};

	CreateConfiguration(nodes, slaveConfiguration, port, serverId, dbPathMaster);

	ServerControl& master = nodes[0];
	TestNamespace1 ns1(master);

	const int startId = 1000;
	const int n2 = 20000;
	auto AddThread = [&master, &ns1]() { ns1.AddRow1msSleep(master, startId, n2, ns1.nsName_); };

	auto restartServer = [this, &nodes, &dbPathMaster, &stopRestartServerThread]() {
		while (!stopRestartServerThread) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			int N = rand() % 3;
			RestartServer(N + 1, nodes, port, dbPathMaster);
		}
	};

	std::thread insertThread(AddThread);
	std::thread restartThread(restartServer);

	insertThread.join();
	stopRestartServerThread = true;
	restartThread.join();
	//---------------------------

	for (size_t i = 1; i < nodes.size(); i++) {
		WaitSync(nodes[0], nodes[i], "ns1");
	}

	std::vector<std::vector<int>> results;

	Query qr = Query("ns1").Sort("id", true);

	for (size_t i = 0; i < nodes.size(); i++) {
		results.push_back(vector<int>());
		ns1.GetData(nodes[i], results.back());
	}

	for (size_t i = 1; i < results.size(); ++i) {
		EXPECT_TRUE((results[0] == results[i])) << i << "; size[0]: " << results[0].size() << "; size[i]: " << results[i].size();
	}
}

#endif

TEST_F(ReplicationSlaveSlaveApi, TransactionTest) {
	const int port = 9999;
	std::string dbPathMaster("/tmp/test_slave4/test_");
	reindexer::fs::RmDirAll("/tmp/test_slave4/");
	const int serverId = 5;
	/*
			m
			|
			1
			|
			2
			|
			3
			|
			4
	*/
	std::vector<int> slaveConfiguration = {-1, port, port + 1, port + 2, port + 3};

	vector<ServerControl> nodes;

	CreateConfiguration(nodes, slaveConfiguration, port, serverId, dbPathMaster);

	size_t kRows = 100;
	string nsName("ns1");

	ServerControl& master = nodes[0];
	TestNamespace1 ns1(master, nsName);

	ns1.AddRows(master, 0, kRows);

	for (size_t i = 1; i < nodes.size(); i++) {
		WaitSync(nodes[0], nodes[i], nsName);
	}

	reindexer::client::Transaction tr = master.Get()->api.reindexer->NewTransaction(nsName);
	for (unsigned int i = 0; i < kRows; i++) {
		reindexer::client::Item item = tr.NewItem();
		auto err = item.FromJSON("{\"id\":" + std::to_string(i + kRows * 10) + "}");
		tr.Upsert(std::move(item));
	}
	master.Get()->api.reindexer->CommitTransaction(tr);

	for (size_t i = 1; i < nodes.size(); i++) {
		WaitSync(nodes[0], nodes[i], nsName);
	}

	std::vector<std::vector<int>> results;
	for (size_t i = 0; i < nodes.size(); i++) {
		results.push_back(vector<int>());
		ns1.GetData(nodes[i], results.back());
	}

	for (size_t i = 1; i < results.size(); ++i) {
		EXPECT_TRUE((results[0] == results[i]));
	}
}

TEST_F(ReplicationSlaveSlaveApi, ForceSync3Node) {
	reindexer::fs::RmDirAll("/tmp/testForceSync");
	ServerControl master;
	master.InitServer(0, 7770, 7880, "/tmp/testForceSync/master", "db");
	TestNamespace1 testns(master);
	testns.AddRows(master, 10, 1000);

	master.Get()->MakeMaster();

	ServerControl slave1;
	slave1.InitServer(0, 7771, 7881, "/tmp/testForceSync/slave1", "db");
	std::string upDsn1 = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest configSlave1("slave", false, true, 0, upDsn1);
	slave1.Get()->MakeMaster();

	ServerControl slave2;
	slave2.InitServer(0, 7772, 7882, "/tmp/testForceSync/slave2", "db");
	std::string upDsn2 = "cproto://127.0.0.1:7771/db";
	ReplicationConfigTest configSlave2("slave", false, true, 0, upDsn2);
	slave2.Get()->MakeSlave(0, configSlave2);

	ServerControl slave3;
	slave3.InitServer(0, 7773, 7883, "/tmp/testForceSync/slave3", "db");
	std::string upDsn3 = "cproto://127.0.0.1:7772/db";
	ReplicationConfigTest configSlave3("slave", false, true, 0, upDsn3);
	slave3.Get()->MakeSlave(0, configSlave3);

	slave1.Get()->MakeSlave(0, configSlave1);

	WaitSync(master, slave1, "ns1");
	WaitSync(master, slave2, "ns1");
	WaitSync(master, slave3, "ns1");

	std::vector<int> results_m;
	testns.GetData(master, results_m);

	std::vector<int> results_s1;
	testns.GetData(slave1, results_s1);

	std::vector<int> results_s2;
	testns.GetData(slave2, results_s2);

	std::vector<int> results_s3;
	testns.GetData(slave3, results_s3);

	EXPECT_TRUE(results_m == results_s1);
	EXPECT_TRUE(results_m == results_s2);
	EXPECT_TRUE(results_m == results_s3);
}

TEST_F(ReplicationSlaveSlaveApi, NodeWithMasterAndSlaveNs1) {
	reindexer::fs::RmDirAll("/tmp/testNsMasterSlave");
	ServerControl master;
	master.InitServer(0, 7770, 7880, "/tmp/testNsMasterSlave/master", "db");
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, 11, 113);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, 11, 113);
	master.Get()->MakeMaster();

	const unsigned int c1 = 5011;
	const unsigned int c2 = 6013;
	const unsigned int n = 121;
	ServerControl slave;
	slave.InitServer(0, 7771, 7881, "/tmp/testNsMasterSlave/slave", "db");
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest configSlave("slave", false, true, 0, upDsn);
	slave.Get()->MakeSlave(0, configSlave);
	testns3.AddRows(slave, c2, n);

	WaitSync(master, slave, "ns1");
	WaitSync(master, slave, "ns2");
	{
		std::vector<int> results_m;
		testns1.GetData(master, results_m);

		std::vector<int> results_s1;
		testns1.GetData(slave, results_s1);
		EXPECT_TRUE(results_m == results_s1);
	}
	{
		std::vector<int> results_m;
		testns2.GetData(master, results_m);

		std::vector<int> results_s1;
		testns2.GetData(slave, results_s1);
		EXPECT_TRUE(results_m == results_s1);
	}
	{
		std::vector<int> results_data;
		for (unsigned int i = 0; i < n; i++) results_data.push_back(c1 + i);
		for (unsigned int i = 0; i < n; i++) results_data.push_back(c2 + i);

		std::vector<int> results_3;
		testns3.GetData(slave, results_3);
		EXPECT_TRUE(results_data == results_3);
	}
}

TEST_F(ReplicationSlaveSlaveApi, NodeWithMasterAndSlaveNs2) {
	const unsigned int cm1 = 11;
	const unsigned int cm2 = 999;
	const unsigned int cm3 = 1999;
	const unsigned int nm = 113;

	reindexer::fs::RmDirAll("/tmp/testNsMasterSlave");
	ServerControl master;
	master.InitServer(0, 7770, 7880, "/tmp/testNsMasterSlave/master", "db");
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, cm1, nm);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, cm2, nm);
	master.Get()->MakeMaster();

	const unsigned int c1 = 5001;
	const unsigned int c2 = 6007;
	const unsigned int n = 101;
	ServerControl slave;
	slave.InitServer(0, 7771, 7881, "/tmp/testNsMasterSlave/slave", "db");
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	TestNamespace1 testns4(slave, "ns1");
	testns4.AddRows(slave, c1, n);
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest::NsSet nsSet = {"ns1"};
	ReplicationConfigTest configSlave("slave", false, true, 0, upDsn, "slave", nsSet);
	slave.Get()->MakeSlave(0, configSlave);
	testns3.AddRows(slave, c2, n);

	WaitSync(master, slave, "ns1");

	testns1.AddRows(master, cm3, nm);
	testns2.AddRows(master, cm2, nm);

	WaitSync(master, slave, "ns1");

	{
		std::vector<int> results_m;
		testns1.GetData(master, results_m);

		std::vector<int> results_s1;
		testns1.GetData(slave, results_s1);
		EXPECT_TRUE(results_m == results_s1);
	}
	{
		std::vector<int> results_data;
		for (unsigned int i = 0; i < n; i++) results_data.push_back(c1 + i);
		for (unsigned int i = 0; i < n; i++) results_data.push_back(c2 + i);

		std::vector<int> results_3;
		testns3.GetData(slave, results_3);
		EXPECT_TRUE(results_data == results_3);
	}
}

TEST_F(ReplicationSlaveSlaveApi, NodeWithMasterAndSlaveNs3) {
	reindexer::fs::RmDirAll("/tmp/testNsMasterSlave");
	const unsigned int c1 = 5001;
	const unsigned int c2 = 6001;
	const unsigned int n = 101;
	ServerControl master;
	master.InitServer(0, 7770, 7880, "/tmp/testNsMasterSlave/master", "db");
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, 11, n);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, 11, n);
	master.Get()->MakeMaster();

	ServerControl slave;
	slave.InitServer(0, 7771, 7881, "/tmp/testNsMasterSlave/slave", "db");
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	TestNamespace1 testns4(slave, "ns1");
	testns4.AddRows(slave, c1, n);
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest::NsSet nsSet = {"ns1"};
	ReplicationConfigTest configSlave("slave", false, true, 0, upDsn, "slave", nsSet);
	slave.Get()->MakeSlave(0, configSlave);
	testns3.AddRows(slave, c2, n);

	WaitSync(master, slave, "ns1");

	slave.Get()->MakeMaster();
	testns4.AddRows(slave, c1 + c2, n);
	{
		std::vector<int> results_m;
		testns4.GetData(slave, results_m);
		EXPECT_TRUE(results_m.size() == n * 2);
	}
}
