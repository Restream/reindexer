#include <chrono>
#include "rpcclient_api.h"
#include "rpcserver_fake.h"

using std::chrono::seconds;

TEST_F(RPCClientTestApi, ConnectTimeout) {
	StartFakeServer();
	reindexer::client::ReindexerConfig config;
	config.ConnectTimeout = seconds(1);
	config.RequestTimeout = seconds(5);
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
	config.ConnectTimeout = seconds(3);
	config.RequestTimeout = seconds(3);
	client_.reset(new reindexer::client::Reindexer(config));
	auto res = client_->Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());
	const string kNamespaceName = "MyNamespace";
	res = client_->AddNamespace(reindexer::NamespaceDef(kNamespaceName));
	EXPECT_EQ(res.code(), errTimeout);
	res = client_->DropNamespace(kNamespaceName);
	EXPECT_TRUE(res.ok()) << res.what();
	StopServer();
}

TEST_F(RPCClientTestApi, SuccessfullRequestWithTimeout) {
	StartFakeServer();
	reindexer::client::ReindexerConfig config;
	config.ConnectTimeout = seconds(3);
	config.RequestTimeout = seconds(6);
	client_.reset(new reindexer::client::Reindexer(config));
	auto res = client_->Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());
	res = client_->AddNamespace(reindexer::NamespaceDef("MyNamespace"));
	EXPECT_TRUE(res.ok());
	StopServer();
}
