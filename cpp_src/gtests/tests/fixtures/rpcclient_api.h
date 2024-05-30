#pragma once

#include <gtest/gtest.h>
#include <thread>
#include "rpcserver_fake.h"

#include "client/reindexer.h"
#include "replicator/updatesobserver.h"
#include "server/server.h"
#include "tools/fsops.h"

#include "client/cororeindexer.h"

inline const std::string kDefaultRPCPort = "25673";
inline const std::string kDefaultRPCServerAddr = "127.0.0.1:" + kDefaultRPCPort;
constexpr uint16_t kDefaultHttpPort = 33333;

class RPCClientTestApi : public ::testing::Test {
public:
	RPCClientTestApi() {}
	virtual ~RPCClientTestApi() {
		[[maybe_unused]] auto err = StopAllServers();
		assertf(err.ok(), "%s", err.what());
	}

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
		void Start(const std::string& addr, Error errOnLogin = Error());
		void Stop();
		const std::string& GetDsn() const { return dsn_; }
		RPCServerStatus Status() const { return server_->Status(); }
		Error const& ErrorStatus() const { return err_; }
		size_t CloseQRRequestsCount() const { return server_->CloseQRRequestsCount(); }

	private:
		std::unique_ptr<RPCServerFake> server_;
		std::unique_ptr<std::thread> serverThread_;
		net::ev::dynamic_loop loop_;
		net::ev::async stop_;
		std::atomic<bool> terminate_;
		std::atomic<bool> serverIsReady_;
		std::string dsn_;
		RPCServerConfig conf_;
		Error err_{errOK};
	};

	class UpdatesReciever : public IUpdatesObserver {
	public:
		UpdatesReciever(ev::dynamic_loop& loop) : loop_(loop) {}

		void OnWALUpdate(LSNPair, std::string_view nsName, const WALRecord&) override final;
		void OnConnectionState(const Error&) override final {}
		void OnUpdatesLost(std::string_view) override final {}

		using map = tsl::hopscotch_map<std::string, size_t, nocase_hash_str, nocase_equal_str>;
		// using map = std::unordered_map<std::string, size_t>;

		const map& Counters() const;
		void Reset();
		void Dump() const;
		bool AwaitNamespaces(size_t count);
		bool AwaitItems(std::string_view ns, size_t count);

	private:
		map updatesCounters_;
		ev::dynamic_loop& loop_;
	};

	void SetUp() {}
	void TearDown() {
		for (auto& fs : fakeServers_) {
			fs.second->Stop();
		}
		for (auto& s : realServers_) {
			if (s.second.serverThread) {
				s.second.server->Stop();
				assertrx(s.second.serverThread->joinable());
				s.second.serverThread->join();
				s.second.serverThread.reset();
			}
		}
		fakeServers_.clear();
		realServers_.clear();
	}

	void StartDefaultRealServer();
	TestServer& AddFakeServer(const std::string& addr = kDefaultRPCServerAddr, const RPCServerConfig& conf = RPCServerConfig());
	void AddRealServer(const std::string& dbPath, const std::string& addr = kDefaultRPCServerAddr, uint16_t httpPort = kDefaultHttpPort);
	void StartServer(const std::string& addr = kDefaultRPCServerAddr, Error errOnLogin = Error());
	Error StopServer(const std::string& addr = kDefaultRPCServerAddr);	// -V1071
	bool CheckIfFakeServerConnected(const std::string& addr = kDefaultRPCServerAddr);
	Error StopAllServers();
	client::Item CreateItem(reindexer::client::Reindexer& rx, std::string_view nsName, int id);
	client::Item CreateItem(reindexer::client::CoroReindexer& rx, std::string_view nsName, int id);
	void CreateNamespace(reindexer::client::Reindexer& rx, std::string_view nsName);
	void CreateNamespace(reindexer::client::CoroReindexer& rx, std::string_view nsName);
	void FillData(reindexer::client::Reindexer& rx, std::string_view nsName, int from, int count);
	void FillData(reindexer::client::CoroReindexer& rx, std::string_view nsName, int from, int count);

	const std::string kDbPrefix{reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/rpc_client_test")};

private:
	struct ServerData {
		ServerData() { server.reset(new reindexer_server::Server()); }

		std::unique_ptr<reindexer_server::Server> server;
		std::unique_ptr<std::thread> serverThread;
	};

	std::unordered_map<std::string, std::unique_ptr<TestServer>> fakeServers_;
	std::unordered_map<std::string, ServerData> realServers_;
};
