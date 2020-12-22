#include "rpcclient_api.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"
#include "tools/stringstools.h"

void RPCClientTestApi::TestServer::Start(const std::string& addr, Error errOnLogin) {
	dsn_ = addr;
	serverThread_.reset(new thread([this, addr, errOnLogin]() {
		server_.reset(new RPCServerFake(conf_));
		stop_.set(loop_);
		stop_.set([&](ev::async& sig) { sig.loop.break_loop(); });
		stop_.start();
		bool res = server_->Start(addr, loop_, errOnLogin);
		assert(res);
		(void)res;
		serverIsReady_ = true;
		while (!terminate_) {
			loop_.run();
		}
		serverIsReady_ = false;
		stop_.stop();
		server_->Stop();
		server_.reset();
	}));
	while (!serverIsReady_) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}

void RPCClientTestApi::TestServer::Stop() {
	if (serverThread_) {
		terminate_ = true;
		stop_.send();
		if (serverThread_->joinable()) {
			serverThread_->join();
		}
		serverThread_.reset();
		terminate_ = false;
		dsn_.clear();
	}
}

void RPCClientTestApi::StartDefaultRealServer() {
	const std::string dbPath = string(kDbPrefix) + "/" + kDefaultRPCPort;
	reindexer::fs::RmDirAll(dbPath);
	AddRealServer(dbPath);
	StartServer();
}

void RPCClientTestApi::AddFakeServer(const std::string& addr, const RPCServerConfig& conf) {
	fakeServers_.emplace(addr, std::unique_ptr<TestServer>(new TestServer(conf)));
}

void RPCClientTestApi::AddRealServer(const std::string& dbPath, const std::string& addr, uint16_t httpPort) {
	auto res = realServers_.emplace(addr, ServerData());
	ASSERT_TRUE(res.second);
	// clang-format off
	std::string yaml =
			"storage:\n"
			"    path:" + dbPath  +"\n"
			"metrics:\n"
			"   clientsstats: true\n"
			"logger:\n"
			"   loglevel: none\n"
			"   rpclog: \n"
			"   serverlog: \n"
			"net:\n"
			"   rpcaddr: " + addr + "\n"
			"   httpaddr: 0.0.0.0:" + std::to_string(httpPort);
	// clang-format on
	Error err = res.first->second.server->InitFromYAML(yaml);
	ASSERT_TRUE(err.ok()) << err.what();
}

void RPCClientTestApi::StartServer(const std::string& addr, Error errOnLogin) {
	{
		auto it = fakeServers_.find(addr);
		if (it != fakeServers_.end()) {
			it->second->Start(addr, errOnLogin);
			return;
		}
	}
	{
		auto it = realServers_.find(addr);
		if (it != realServers_.end()) {
			if (it->second.serverThread && it->second.serverThread->joinable()) {
				assert(it->second.server->IsReady());
				assert(it->second.server->IsRunning());
				return;
			}
			it->second.serverThread.reset(new std::thread(
				[](reindexer_server::Server* server) {
					int res = server->Start();
					assert(res == EXIT_SUCCESS);
					(void)res;
				},
				it->second.server.get()));
			while (!it->second.server->IsRunning() || !it->second.server->IsReady()) {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
			return;
		}
	}
	assertf(false, "Server with dsn %s was not found", addr);
}

void RPCClientTestApi::StopServer(const std::string& addr) {
	{
		auto it = fakeServers_.find(addr);
		if (it != fakeServers_.end()) {
			it->second->Stop();
			return;
		}
	}
	{
		auto it = realServers_.find(addr);
		if (it != realServers_.end()) {
			it->second.server->Stop();
			assert(it->second.serverThread);
			assert(it->second.serverThread->joinable());
			it->second.serverThread->join();
			it->second.serverThread.reset();
			return;
		}
	}
	assertf(false, "Server with dsn %s was not found", addr);
}

bool RPCClientTestApi::CheckIfFakeServerConnected(const std::string& addr) {
	auto it = fakeServers_.find(addr);
	if (it != fakeServers_.end()) {
		return (it->second->Status() == RPCServerStatus::Connected);
	}
	return false;
}

void RPCClientTestApi::StopAllServers() {
	for (const auto& item : fakeServers_) {
		item.second->Stop();
	}
	for (auto& item : realServers_) {
		if (item.second.serverThread && item.second.serverThread->joinable()) {
			item.second.server->Stop();
			item.second.serverThread->join();
			item.second.serverThread.reset();
		}
	}
}

client::Item RPCClientTestApi::CreateItem(client::Reindexer& rx, string_view nsName, int id) {
	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder jb(wrser);
	jb.Put("id", id);
	jb.End();
	auto item = rx.NewItem(nsName);
	EXPECT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(wrser.Slice());
	EXPECT_TRUE(err.ok()) << err.what();
	return item;
}

client::Item RPCClientTestApi::CreateItem(client::CoroReindexer& rx, string_view nsName, int id) {
	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder jb(wrser);
	jb.Put("id", id);
	jb.End();
	auto item = rx.NewItem(nsName);
	EXPECT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(wrser.Slice());
	EXPECT_TRUE(err.ok()) << err.what();
	return item;
}

void RPCClientTestApi::CreateNamespace(reindexer::client::Reindexer& rx, string_view nsName) {
	auto err = rx.OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.AddIndex(nsName, {"id", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();
}

void RPCClientTestApi::CreateNamespace(client::CoroReindexer& rx, string_view nsName) {
	auto err = rx.OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.AddIndex(nsName, {"id", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();
}

void RPCClientTestApi::FillData(client::Reindexer& rx, string_view nsName, int from, int count) {
	int to = from + count;
	for (int id = from; id < to; ++id) {
		auto item = CreateItem(rx, nsName, id);
		auto err = rx.Upsert(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

void RPCClientTestApi::FillData(client::CoroReindexer& rx, string_view nsName, int from, int count) {
	int to = from + count;
	for (int id = from; id < to; ++id) {
		auto item = CreateItem(rx, nsName, id);
		auto err = rx.Upsert(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

void RPCClientTestApi::UpdatesReciever::OnWALUpdate(LSNPair, string_view nsName, const WALRecord&) {
	auto found = updatesCounters_.find(nsName);
	if (found != updatesCounters_.end()) {
		++(found->second);
	} else {
		updatesCounters_.emplace(string(nsName), 1);
	}
}

const RPCClientTestApi::UpdatesReciever::map& RPCClientTestApi::UpdatesReciever::Counters() const { return updatesCounters_; }

void RPCClientTestApi::UpdatesReciever::Reset() { updatesCounters_.clear(); }

void RPCClientTestApi::UpdatesReciever::Dump() const {
	std::cerr << "Reciever dump: " << std::endl;
	for (auto& it : Counters()) {
		std::cerr << it.first << ": " << it.second << std::endl;
	}
}

bool RPCClientTestApi::UpdatesReciever::AwaitNamespaces(size_t count) {
	std::chrono::milliseconds time{0};
	auto cycleTime = std::chrono::milliseconds(50);
	while (Counters().size() != count) {
		time += cycleTime;
		if (time >= std::chrono::seconds(5)) {
			Dump();
			return false;
		}
		loop_.sleep(cycleTime);
	}
	return true;
}

bool RPCClientTestApi::UpdatesReciever::AwaitItems(string_view ns, size_t count) {
	std::chrono::milliseconds time{0};
	auto cycleTime = std::chrono::milliseconds(50);
	do {
		auto counters = Counters();
		time += cycleTime;
		if (time >= std::chrono::seconds(5)) {
			Dump();
			return false;
		}

		auto found = counters.find(ns);
		if (found != counters.end() && found->second == count) {
			return true;
		}
		loop_.sleep(cycleTime);
	} while (true);
}
