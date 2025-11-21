#pragma once

#include "core/cjson/jsonbuilder.h"
#include "core/enums.h"
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "reindexer_api.h"
#include "rpc_test_client.h"
#include "server/server.h"
#include "tools/dsn.h"
#include "tools/fsops.h"
#include "yaml-cpp/yaml.h"

class [[nodiscard]] MsgPackCprotoApi : public ReindexerApi {
public:
	using Reindexer = reindexer::client::Reindexer;
	using QueryResults = reindexer::client::QueryResults;
	using Item = reindexer::client::Item;

	MsgPackCprotoApi() = default;
	~MsgPackCprotoApi() = default;

	void SetUp() {
		using reindexer::client::RPCDataFormat;
		std::ignore = reindexer::fs::RmDirAll(kDbPath);
		YAML::Node y;
		y["storage"]["path"] = kDbPath;
		y["logger"]["loglevel"] = "none";
		y["logger"]["rpclog"] = "none";
		y["logger"]["serverlog"] = "none";
		y["net"]["httpaddr"] = "0.0.0.0:44444";
		y["net"]["rpcaddr"] = "0.0.0.0:25677";

		auto err = server_.InitFromYAML(YAML::Dump(y));
		ASSERT_TRUE(err.ok()) << err.what();

		serverThread_ = std::unique_ptr<std::thread>(new std::thread([this]() {
			int status = this->server_.Start();
			ASSERT_TRUE(status == EXIT_SUCCESS) << status;
		}));

		while (!server_.IsReady() || !server_.IsRunning()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}

		client_.reset(new reindexer::client::RPCTestClient());
		err = client_->Connect(reindexer::DSN("cproto://127.0.0.1:25677/" + kDbName), reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->OpenNamespace(default_namespace, StorageOpts().CreateIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->AddIndex(default_namespace, reindexer::IndexDef(kFieldId, "hash", "int", IndexOpts().PK()));
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->AddIndex(default_namespace, reindexer::IndexDef(kFieldA1, "hash", "int", IndexOpts()));
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->AddIndex(default_namespace, reindexer::IndexDef(kFieldA2, "hash", "int", IndexOpts()));
		ASSERT_TRUE(err.ok()) << err.what();

		err = client_->AddIndex(default_namespace, reindexer::IndexDef(kFieldA3, "hash", "int", IndexOpts()));
		ASSERT_TRUE(err.ok()) << err.what();

		reindexer::WrSerializer wrser;
		for (size_t i = 0; i < 1000; ++i) {
			wrser.Reset();
			reindexer::JsonBuilder jsonBuilder(wrser, reindexer::ObjType::TypeObject);
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

			err = client_->Upsert(default_namespace, item, RPCDataFormat::CJSON);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	void TearDown() {
		if (server_.IsRunning()) {
			server_.Stop();
		}
		client_.reset();
		if (serverThread_->joinable()) {
			serverThread_->join();
		}
		serverThread_.reset();
	}

	void checkItem(reindexer::client::QueryResults::Iterator& it) {
		reindexer::client::Item item = it.GetItem();
		std::string json(item.GetJSON());
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		reindexer::WrSerializer buf;
		Error err = it.GetMsgPack(buf, false);
		ASSERT_TRUE(err.ok()) << err.what();

		size_t offset = 0;
		reindexer::client::Item simulatedItem = client_->NewItem(default_namespace);
		err = simulatedItem.FromMsgPack(buf.Slice(), offset);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(json.compare(simulatedItem.GetJSON()) == 0);
	}

protected:
	const std::string kDbName = "cproto_msgpack_test";
	const std::string kDbPath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/" + kDbName);
	const std::string kFieldId = "id";
	const std::string kFieldA1 = "a1";
	const std::string kFieldA2 = "a2";
	const std::string kFieldA3 = "a3";

	reindexer_server::Server server_;
	std::unique_ptr<std::thread> serverThread_;
	std::unique_ptr<reindexer::client::RPCTestClient> client_;
};
