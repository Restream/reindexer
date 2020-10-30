#include <chrono>
#include "rpcclient_api.h"
#include "rpcserver_fake.h"

using std::chrono::seconds;

TEST_F(RPCClientTestApi, ConnectTimeout) {
	AddServer();
	StartServer();
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
	AddServer();
	StartServer();
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

TEST_F(RPCClientTestApi, RequestCancels) {
	AddServer();
	StartServer();
	client_.reset(new reindexer::client::Reindexer);
	auto res = client_->Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());

	{
		CancelRdxContext ctx;
		ctx.Cancel();
		res = client_->WithContext(&ctx).AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_EQ(res.code(), errCanceled);
	}

	{
		CancelRdxContext ctx;
		std::thread thr([this, &ctx] {
			auto res = client_->WithContext(&ctx).AddNamespace(reindexer::NamespaceDef("MyNamespace"));
			EXPECT_EQ(res.code(), errCanceled);
		});

		std::this_thread::sleep_for(std::chrono::seconds(1));
		ctx.Cancel();
		thr.join();
	}

	StopServer();
}

TEST_F(RPCClientTestApi, SuccessfullRequestWithTimeout) {
	AddServer();
	StartServer();
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

TEST_F(RPCClientTestApi, ErrorLoginResponse) {
	AddServer();
	StartServer(kDefaultRPCServerAddr, errForbidden);
	client_.reset(new reindexer::client::Reindexer());
	client_->Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	auto res = client_->AddNamespace(reindexer::NamespaceDef("MyNamespace"));
	EXPECT_EQ(res.code(), errForbidden);
	StopServer();
}

TEST_F(RPCClientTestApi, SeveralDsnReconnect) {
	const string cprotoIdentifier = "cproto://";
	const string dbName = "/test_db";
	const vector<string> uris = {"127.0.0.1:25673", "127.0.0.1:25674", "127.0.0.1:25675", "127.0.0.1:25676"};

	RPCServerConfig serverConfig;
	serverConfig.loginDelay = std::chrono::milliseconds(1);
	serverConfig.openNsDelay = std::chrono::milliseconds(1);
	serverConfig.selectDelay = std::chrono::milliseconds(1);
	for (const string& uri : uris) {
		AddServer(uri, serverConfig);
		StartServer(uri);
	}

	reindexer::client::ReindexerConfig clientConfig;
	clientConfig.ConnectTimeout = seconds(10);
	clientConfig.RequestTimeout = seconds(10);
	clientConfig.ReconnectAttempts = 0;
	client_.reset(new reindexer::client::Reindexer(clientConfig));
	std::vector<pair<string, reindexer::client::ConnectOpts>> connectData;
	for (const string& uri : uris) {
		connectData.emplace_back(string(cprotoIdentifier + uri + dbName), reindexer::client::ConnectOpts());
	}
	auto res = client_->Connect(connectData);
	EXPECT_TRUE(res.ok()) << res.what();

	for (size_t i = 0; i < 100; ++i) {
		if (CheckIfConnected(uris[0])) break;
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	for (size_t i = 0; i < 100; ++i) {
		if (client_->Status().ok()) break;
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	Query queryConfingNs = Query("#config");
	for (size_t i = 0; i < uris.size() - 1; ++i) {
		StopServer(uris[i]);
		for (size_t j = 0; j < 10; ++j) {
			client::QueryResults qr;
			res = client_->Select(queryConfingNs, qr);
			if (res.ok()) break;
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		EXPECT_TRUE(res.ok()) << res.what();
	}
	StopAllServers();
}
