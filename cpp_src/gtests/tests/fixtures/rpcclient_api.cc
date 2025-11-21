#include "rpcclient_api.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"
#include "yaml-cpp/yaml.h"

const std::string RPCClientTestApi::kDefaultRPCServerAddr = "127.0.0.1:" + std::to_string(RPCClientTestApi::kDefaultRPCPort);

void RPCClientTestApi::TestServer::Start(const std::string& addr, Error errOnLogin) {
	dsn_ = DSN(addr);
	serverThread_.reset(new std::thread([this, addr, errOnLogin = std::move(errOnLogin)]() {
		server_.reset(new RPCServerFake(conf_));
		stop_.set(loop_);
		stop_.set([&](ev::async& sig) { sig.loop.break_loop(); });
		stop_.start();
		server_->Start(addr, loop_, errOnLogin);
		serverIsReady_ = true;
		while (!terminate_) {
			loop_.run();
		}
		serverIsReady_ = false;
		stop_.stop();
		Error err = server_->Stop();
		if (!err.ok() && err_.ok()) {
			err_ = err;
		}
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
		dsn_ = {};
	}
}

void RPCClientTestApi::StartDefaultRealServer() {
	const std::string dbPath = std::string(kDbPrefix) + "/" + std::to_string(kDefaultRPCPort);
	std::ignore = reindexer::fs::RmDirAll(dbPath);
	AddRealServer(dbPath);
	StartServer();
}

RPCClientTestApi::TestServer& RPCClientTestApi::AddFakeServer(const std::string& addr, const RPCServerConfig& conf) {
	fakeServers_.emplace(addr, std::unique_ptr<TestServer>(new TestServer(conf)));
	return *fakeServers_[addr];
}

void RPCClientTestApi::AddRealServer(const std::string& dbPath, const std::string& addr, uint16_t httpPort) {
	auto res = realServers_.emplace(addr, ServerData());
	ASSERT_TRUE(res.second);
	YAML::Node y;
	y["storage"]["path"] = dbPath;
	y["metrics"]["clientsstats"] = true;
	y["logger"]["loglevel"] = "none";
	y["logger"]["rpclog"] = "none";
	y["logger"]["serverlog"] = "none";
	y["net"]["httpaddr"] = "0.0.0.0:" + std::to_string(httpPort);
	y["net"]["rpcaddr"] = addr;

	Error err = res.first->second.server->InitFromYAML(YAML::Dump(y));
	ASSERT_TRUE(err.ok()) << err.what();
}

void RPCClientTestApi::StartServer(const std::string& addr, Error errOnLogin) {
	{
		auto it = fakeServers_.find(addr);
		if (it != fakeServers_.end()) {
			it->second->Start(addr, std::move(errOnLogin));
			return;
		}
	}
	{
		auto it = realServers_.find(addr);
		if (it != realServers_.end()) {
			if (it->second.serverThread && it->second.serverThread->joinable()) {
				assertrx(it->second.server->IsReady());
				assertrx(it->second.server->IsRunning());
				return;
			}
			it->second.serverThread.reset(new std::thread(
				[](reindexer_server::Server* server) {
					int res = server->Start();
					assertrx(res == EXIT_SUCCESS);
					(void)res;
				},
				it->second.server.get()));
			while (!it->second.server->IsRunning() || !it->second.server->IsReady()) {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
			return;
		}
	}
	assertf(false, "Server with dsn {} was not found", addr);
}

Error RPCClientTestApi::StopServer(const std::string& addr) {
	{
		auto it = fakeServers_.find(addr);
		if (it != fakeServers_.end()) {
			it->second->Stop();
			return it->second->ErrorStatus();
		}
	}
	{
		auto it = realServers_.find(addr);
		if (it != realServers_.end()) {
			it->second.server->Stop();
			assertrx(it->second.serverThread);
			assertrx(it->second.serverThread->joinable());
			it->second.serverThread->join();
			it->second.serverThread.reset();
			return errOK;
		}
	}
	assertf(false, "Server with dsn {} was not found", addr);
	abort();
}

bool RPCClientTestApi::CheckIfFakeServerConnected(const std::string& addr) {
	auto it = fakeServers_.find(addr);
	if (it != fakeServers_.end()) {
		return (it->second->Status() == RPCServerStatus::Connected);
	}
	return false;
}

Error RPCClientTestApi::StopAllServers() {
	Error res{errOK};
	for (const auto& item : fakeServers_) {
		item.second->Stop();
		if (!item.second->ErrorStatus().ok() && res.ok()) {
			res = item.second->ErrorStatus();
		}
	}
	for (auto& item : realServers_) {
		if (item.second.serverThread && item.second.serverThread->joinable()) {
			item.second.server->Stop();
			item.second.serverThread->join();
			item.second.serverThread.reset();
		}
	}
	return res;
}

client::Item RPCClientTestApi::CreateItem(client::CoroReindexer& rx, std::string_view nsName, int id) {
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

void RPCClientTestApi::CreateNamespace(client::CoroReindexer& rx, std::string_view nsName) {
	auto err = rx.OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.AddIndex(nsName, {"id", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();
}

void RPCClientTestApi::FillData(client::CoroReindexer& rx, std::string_view nsName, int from, int count) {
	int to = from + count;
	for (int id = from; id < to; ++id) {
		auto item = CreateItem(rx, nsName, id);
		auto err = rx.Upsert(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}
