#pragma once

#include <gtest/gtest.h>
#include <thread>
#include "client/cororeindexer.h"
#include "rpcserver_fake.h"
#include "server/server.h"
#include "tools/dsn.h"
#include "tools/fsops.h"

class [[nodiscard]] RPCClientTestApi : public ::testing::Test {
public:
	virtual ~RPCClientTestApi() = default;

protected:
	class [[nodiscard]] CancelRdxContext : public reindexer::IRdxCancelContext {
	public:
		reindexer::CancelType GetCancelType() const noexcept override {
			return canceld_.load() ? reindexer::CancelType::Explicit : reindexer::CancelType::None;
		}
		bool IsCancelable() const noexcept override { return true; }
		std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }
		void Cancel() { canceld_ = true; }

	private:
		std::atomic<bool> canceld_ = {false};
	};

	class [[nodiscard]] TestServer {
	public:
		TestServer(const RPCServerConfig& conf) : terminate_(false), serverIsReady_(false), conf_(conf) {}
		void Start(const std::string& addr, Error errOnLogin = Error());
		void Stop();
		const DSN& GetDsn() const { return dsn_; }
		RPCServerStatus Status() const { return server_->Status(); }
		const Error& ErrorStatus() const { return err_; }
		size_t CloseQRRequestsCount() const { return server_->CloseQRRequestsCount(); }

	private:
		std::unique_ptr<RPCServerFake> server_;
		std::unique_ptr<std::thread> serverThread_;
		net::ev::dynamic_loop loop_;
		net::ev::async stop_;
		std::atomic<bool> terminate_;
		std::atomic<bool> serverIsReady_;
		DSN dsn_;
		RPCServerConfig conf_;
		Error err_{errOK};
	};

	void SetUp() {}
	void TearDown() {
		[[maybe_unused]] auto err = StopAllServers();
		assertf(err.ok(), "{}", err.what());
		fakeServers_.clear();
		realServers_.clear();
	}

public:
	void StartDefaultRealServer();
	TestServer& AddFakeServer(const std::string& addr = kDefaultRPCServerAddr, const RPCServerConfig& conf = RPCServerConfig());
	void AddRealServer(const std::string& dbPath, const std::string& addr = kDefaultRPCServerAddr, uint16_t httpPort = kDefaultHttpPort);
	void StartServer(const std::string& addr = kDefaultRPCServerAddr, Error errOnLogin = Error());
	Error StopServer(const std::string& addr = kDefaultRPCServerAddr);	// -V1071
	bool CheckIfFakeServerConnected(const std::string& addr = kDefaultRPCServerAddr);
	Error StopAllServers();
	client::Item CreateItem(reindexer::client::Reindexer& rx, std::string_view nsName, int id);
	client::Item CreateItem(reindexer::client::CoroReindexer& rx, std::string_view nsName, int id);
	void CreateNamespace(reindexer::client::CoroReindexer& rx, std::string_view nsName);
	void FillData(reindexer::client::CoroReindexer& rx, std::string_view nsName, int from, int count);

	const std::string kDbPrefix{reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/rpc_client_test")};
	static const uint16_t kDefaultRPCPort = 25673;
	static const std::string kDefaultRPCServerAddr;
	static const uint16_t kDefaultHttpPort = 33333;

private:
	struct [[nodiscard]] ServerData {
		ServerData() { server.reset(new reindexer_server::Server()); }

		std::unique_ptr<reindexer_server::Server> server;
		std::unique_ptr<std::thread> serverThread;
	};

	std::unordered_map<std::string, std::unique_ptr<TestServer>> fakeServers_;
	std::unordered_map<std::string, ServerData> realServers_;
};
