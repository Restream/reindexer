#include <chrono>
#include "rpcclient_api.h"
#include "rpcserver_fake.h"

using std::chrono::seconds;

TEST_F(RPCClientTestApi, ConnectTimeout) {
	StartFakeServer();
	reindexer::client::ReindexerConfig config;
	config.ConnectTimeout = seconds(2);
	config.RequestTimeout = seconds(10);
	client_.reset(new reindexer::client::Reindexer(config));
	auto res = client_->Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());
	res = client_->AddNamespace(reindexer::NamespaceDef("MyNamespace"));
	EXPECT_EQ(res.code(), errTimeout);
	StopServer();
}

TEST_F(RPCClientTestApi, RequestTimeout) {
	StartFakeServer();
	reindexer::client::ReindexerConfig config;
	config.ConnectTimeout = seconds(7);
	config.RequestTimeout = seconds(8);
	client_.reset(new reindexer::client::Reindexer(config));
	auto res = client_->Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());
	const string kNamespaceName = "MyNamespace";
	res = client_->AddNamespace(reindexer::NamespaceDef(kNamespaceName));
	EXPECT_EQ(res.code(), errTimeout);
	res = client_->DropNamespace(kNamespaceName);
	EXPECT_TRUE(res.ok());
	StopServer();
}

TEST_F(RPCClientTestApi, SuccessfullRequestWithTimeout) {
	StartFakeServer();
	reindexer::client::ReindexerConfig config;
	config.ConnectTimeout = seconds(7);
	config.RequestTimeout = seconds(15);
	client_.reset(new reindexer::client::Reindexer(config));
	auto res = client_->Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());
	res = client_->AddNamespace(reindexer::NamespaceDef("MyNamespace"));
	EXPECT_TRUE(res.ok());
	StopServer();
}
