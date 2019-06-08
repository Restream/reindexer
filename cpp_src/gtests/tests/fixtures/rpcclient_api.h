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
	RPCClientTestApi() : terminate_(false), serverIsReady_(false) {}
	virtual ~RPCClientTestApi() { StopServer(); }

protected:
	void SetUp() { client_.reset(); }
	void TearDown() {}

	void StartFakeServer(const string& addr = kDefaultRPCServerAddr) {
		serverThread_.reset(new thread([this, addr]() {
			server_.reset(new RPCServerFake);
			stop_.set(loop_);
			stop_.set([&](ev::async& sig) { sig.loop.break_loop(); });
			stop_.start();
			bool res = server_->Start(addr, loop_);
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
	void StopServer() {
		if (serverThread_) {
			terminate_ = true;
			stop_.send();
			if (serverThread_->joinable()) {
				serverThread_->join();
			}
			serverThread_.reset();
			terminate_ = false;
		}
	}

	unique_ptr<reindexer::client::Reindexer> client_;

private:
	unique_ptr<RPCServerFake> server_;
	unique_ptr<thread> serverThread_;
	net::ev::dynamic_loop loop_;
	net::ev::async stop_;
	atomic<bool> terminate_;
	atomic<bool> serverIsReady_;
};
