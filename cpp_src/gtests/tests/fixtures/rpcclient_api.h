#pragma once

#include <gtest/gtest.h>
#include <thread>
#include "rpcserver_fake.h"

#include "client/reindexer.h"

using std::unique_ptr;
using std::atomic;
using std::thread;

const char* const kDefaultRPCServerAddr = "127.0.0.1:25673";

class RPCClientTestApi : public ::testing::Test {
public:
	RPCClientTestApi() {}
	virtual ~RPCClientTestApi() { StopAllServers(); }

protected:
	class TestServer {
	public:
		TestServer(const RPCServerConfig& conf) : terminate_(false), serverIsReady_(false), conf_(conf) {}
		void Start(const string& addr, Error errOnLogin = Error()) {
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
		void Stop() {
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

	void SetUp() { client_.reset(); }
	void TearDown() {}

	void AddServer(const string& addr = kDefaultRPCServerAddr, const RPCServerConfig& conf = RPCServerConfig()) {
		servers_.emplace(addr, new TestServer(conf));
	}
	void StartServer(const string& addr = kDefaultRPCServerAddr, Error errOnLogin = Error()) {
		auto it = servers_.find(addr);
		if (it != servers_.end()) {
			it->second->Start(addr, errOnLogin);
		}
	}
	void StopServer(const string& addr = kDefaultRPCServerAddr) {
		auto it = servers_.find(addr);
		if (it != servers_.end()) {
			it->second->Stop();
		}
	}
	bool CheckIfConnected(const string& addr = kDefaultRPCServerAddr) {
		auto it = servers_.find(addr);
		if (it != servers_.end()) {
			return (it->second->Status() == RPCServerStatus::Connected);
		}
		return false;
	}
	void StopAllServers() {
		for (const auto& item : servers_) {
			item.second->Stop();
		}
	}

	unique_ptr<reindexer::client::Reindexer> client_;

private:
	unordered_map<string, unique_ptr<TestServer>> servers_;
};
