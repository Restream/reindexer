#pragma once

#include <gtest/gtest.h>
#include <thread>
#include "rpcserver_fake.h"

#include "client/reindexer.h"
#include "replicator/updatesobserver.h"
#include "server/server.h"

#include "client/cororeindexer.h"

using std::unique_ptr;
using std::atomic;
using std::thread;

const std::string kDefaultRPCPort = "25673";							   // -V1043
const std::string kDefaultRPCServerAddr = "127.0.0.1:" + kDefaultRPCPort;  // -V1043
constexpr uint16_t kDefaultHttpPort = 33333;

class RPCClientTestApi : public ::testing::Test {
public:
	RPCClientTestApi() {}
	virtual ~RPCClientTestApi() { StopAllServers(); }

protected:
	class CancelRdxContext : public reindexer::IRdxCancelContext {
	public:
		reindexer::CancelType GetCancelType() const noexcept override {
			return canceld_.load() ? reindexer::CancelType::Explicit : reindexer::CancelType::None;
		}
		bool IsCancelable() const noexcept override { return true; }
		void Cancel() { canceld_ = true; }

	private:
		std::atomic<bool> canceld_ = {false};
	};

	class TestServer {
	public:
		TestServer(const RPCServerConfig& conf) : terminate_(false), serverIsReady_(false), conf_(conf) {}
		void Start(const string& addr, Error errOnLogin = Error());
		void Stop();
		const string& GetDsn() const { return dsn_; }
		RPCServerStatus Status() const { return server_->Status(); }

	private:
		unique_ptr<RPCServerFake> server_;
		unique_ptr<thread> serverThread_;
		net::ev::dynamic_loop loop_;
		net::ev::async stop_;
		atomic<bool> terminate_;
		atomic<bool> serverIsReady_;
		string dsn_;
		RPCServerConfig conf_;
	};

	class UpdatesReciever : public IUpdatesObserver {
	public:
		UpdatesReciever(ev::dynamic_loop& loop) : loop_(loop) {}

		void OnWALUpdate(LSNPair, string_view nsName, const WALRecord&) override final;
		void OnConnectionState(const Error&) override final {}
		void OnUpdatesLost(string_view) override final {}

		using map = tsl::hopscotch_map<std::string, size_t, nocase_hash_str, nocase_equal_str>;
		// using map = std::unordered_map<std::string, size_t>;

		const map& Counters() const;
		void Reset();
		void Dump() const;
		bool AwaitNamespaces(size_t count);
		bool AwaitItems(string_view ns, size_t count);

	private:
		map updatesCounters_;
		ev::dynamic_loop& loop_;
	};

	void SetUp() {}
	void TearDown() {}

	void StartDefaultRealServer();
	void AddFakeServer(const string& addr = kDefaultRPCServerAddr, const RPCServerConfig& conf = RPCServerConfig());
	void AddRealServer(const std::string& dbPath, const string& addr = kDefaultRPCServerAddr, uint16_t httpPort = kDefaultHttpPort);
	void StartServer(const string& addr = kDefaultRPCServerAddr, Error errOnLogin = Error());
	void StopServer(const string& addr = kDefaultRPCServerAddr);
	bool CheckIfFakeServerConnected(const string& addr = kDefaultRPCServerAddr);
	void StopAllServers();
	client::Item CreateItem(reindexer::client::Reindexer& rx, string_view nsName, int id);
	client::Item CreateItem(reindexer::client::CoroReindexer& rx, string_view nsName, int id);
	void CreateNamespace(reindexer::client::Reindexer& rx, string_view nsName);
	void CreateNamespace(reindexer::client::CoroReindexer& rx, string_view nsName);
	void FillData(reindexer::client::Reindexer& rx, string_view nsName, int from, int count);
	void FillData(reindexer::client::CoroReindexer& rx, string_view nsName, int from, int count);

	const string_view kDbPrefix = "/tmp/reindex/rpc_client_test"_sv;

private:
	struct ServerData {
		ServerData() { server.reset(new reindexer_server::Server()); }

		std::unique_ptr<reindexer_server::Server> server;
		std::unique_ptr<std::thread> serverThread;
	};

	unordered_map<string, unique_ptr<TestServer>> fakeServers_;
	unordered_map<string, ServerData> realServers_;
};
