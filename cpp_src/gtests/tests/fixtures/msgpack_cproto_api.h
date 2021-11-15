#pragma once

#include "client/internalrdxcontext.h"
#include "client/rpcclientmock.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/objtype.h"
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "reindexer_api.h"
#include "server/dbmanager.h"
#include "server/server.h"
#include "tools/fsops.h"

class MsgPackCprotoApi : public ReindexerApi {
public:
	MsgPackCprotoApi() {}
	~MsgPackCprotoApi() {}

	void SetUp() {
		reindexer::fs::RmDirAll(kDbPath);
		// clang-format off
        const string yaml =
                "storage:\n"
                "    path: /tmp/reindex/" + kDbPath +"\n"
                "logger:\n"
                "   loglevel: none\n"
                "   rpclog: \n"
                "   serverlog: \n"
                "net:\n"
                "   rpcaddr: 0.0.0.0:25677\n"
                "   httpaddr: 0.0.0.0:44444\n"
                "   clusteraddr: 0.0.0.0:45444\n";
		// clang-format on
		auto err = server_.InitFromYAML(yaml);
		ASSERT_TRUE(err.ok()) << err.what();

		serverThread_ = std::unique_ptr<std::thread>(new std::thread([this]() {
			int status = this->server_.Start();
			ASSERT_TRUE(status == EXIT_SUCCESS) << status;
		}));

		while (!server_.IsReady() || !server_.IsRunning()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}

		client_.reset(new reindexer::client::RPCClientMock());
		err = client_->Connect(string("cproto://127.0.0.1:25677/" + kDbPath), reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->OpenNamespace(default_namespace, ctx_, StorageOpts().CreateIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->AddIndex(default_namespace, reindexer::IndexDef(kFieldId, "hash", "int", IndexOpts().PK()), ctx_);
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->AddIndex(default_namespace, reindexer::IndexDef(kFieldA1, "hash", "int", IndexOpts()), ctx_);
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->AddIndex(default_namespace, reindexer::IndexDef(kFieldA2, "hash", "int", IndexOpts()), ctx_);
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->AddIndex(default_namespace, reindexer::IndexDef(kFieldA3, "hash", "int", IndexOpts()), ctx_);
		ASSERT_TRUE(err.ok()) << err.what();

		for (size_t i = 0; i < 1000; ++i) {
			reindexer::WrSerializer wrser;
			reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
			jsonBuilder.Put(kFieldId, i);
			jsonBuilder.Put(kFieldA1, i * 2);
			jsonBuilder.Put(kFieldA2, i * 3);
			jsonBuilder.Put(kFieldA3, i * 4);
			jsonBuilder.End();

			char* endp = nullptr;
			reindexer::client::Item item = client_->NewItem(default_namespace);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			err = item.FromJSON(wrser.Slice(), &endp);
			ASSERT_TRUE(err.ok()) << err.what();

			err = client_->Upsert(default_namespace, item, ctx_);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	void TearDown() {
		if (server_.IsRunning()) {
			server_.Stop();
			if (serverThread_->joinable()) {
				serverThread_->join();
			}
		}
		client_->Stop();
	}

	void checkItem(reindexer::client::QueryResults::Iterator& it) {
		reindexer::client::Item item = it.GetItem();
		string json(item.GetJSON());
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		reindexer::WrSerializer buf;
		Error err = it.GetMsgPack(buf, false);
		ASSERT_TRUE(err.ok()) << err.what();

		size_t offset = 0;
		reindexer::client::Item simulatedItem = client_->NewItem(default_namespace);
		err = simulatedItem.FromMsgPack(buf.Slice(), offset);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(json.compare(string(simulatedItem.GetJSON())) == 0);
	}

protected:
	const string kDbPath = "cproto_msgpack_test";
	const string kFieldId = "id";
	const string kFieldA1 = "a1";
	const string kFieldA2 = "a2";
	const string kFieldA3 = "a3";

	reindexer_server::Server server_;
	std::unique_ptr<std::thread> serverThread_;
	std::unique_ptr<reindexer::client::RPCClientMock> client_;
	reindexer::client::InternalRdxContext ctx_;
};
