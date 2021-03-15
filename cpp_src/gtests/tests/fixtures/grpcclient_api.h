#pragma once

#ifdef WITH_GRPC
#include "reindexer.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include "client/reindexer.h"
#include "core/cjson/cjsonbuilder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/jsonbuilder.h"
#include "core/payload/payloadiface.h"
#include "reindexer_api.h"
#include "tools/fsops.h"

class GrpcClientApi : public ReindexerApi {
public:
	void SetUp() {
		// clang-format off
        string yaml =
            "storage:\n"
            "storage:\n"
            "   path: " + kStoragePath + "\n" +
            "logger:\n"
            "   loglevel: none\n"
            "   rpclog: \n"
            "   serverlog: \n"
            "   corelog: \n"
            "net:\n"
            "   httpaddr: 0.0.0.0:5554\n"
            "   rpcaddr: 0.0.0.0:4443\n"
            "   grpc: true";
		// clang-format on

		reindexer::fs::RmDirAll(kStoragePath);
		auto err = srv_.InitFromYAML(yaml);
		EXPECT_TRUE(err.ok()) << err.what();

		serverThread_ = std::thread([this]() {
			auto res = this->srv_.Start();
			if (res != EXIT_SUCCESS) {
				std::cerr << "Exit code: " << res << std::endl;
			}
			assert(res == EXIT_SUCCESS);
		});
		while (!srv_.IsReady()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}

		for (int i = 0; i < 5; i++) {
			auto channel = grpc::CreateChannel("127.0.0.1:16534", grpc::InsecureChannelCredentials());
			if (channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(1))) {
				rx_ = reindexer::grpc::Reindexer::NewStub(channel);
				break;
			}
		}

		ASSERT_TRUE(rx_);

		grpc::ClientContext createDbCtx;
		reindexer::grpc::CreateDatabaseRequest connectRequest;
		connectRequest.set_dbname(kDbName);
		reindexer::grpc::ErrorResponse errResponse;
		grpc::Status status = rx_->CreateDatabase(&createDbCtx, connectRequest, &errResponse);
		ASSERT_TRUE(status.ok()) << status.error_message();

		reindexer::grpc::AddNamespaceRequest addNsRequest;
		addNsRequest.set_dbname(kDbName);

		reindexer::grpc::Namespace* ns1 = addNsRequest.namespace_().New();
		{
			ns1->set_dbname(kDbName);
			ns1->set_name(default_namespace);
			ns1->set_istemporary(false);

			reindexer::grpc::Index* idx1 = ns1->add_indexesdefinitions();
			idx1->set_name(kIdField);
			idx1->set_fieldtype("int");
			idx1->set_indextype("hash");
			*(idx1->add_jsonpaths()) = "id";
			idx1->set_expireafter(0);
			reindexer::grpc::IndexOptions* opts1 = idx1->options().New();
			opts1->set_ispk(true);
			opts1->set_collatemode(reindexer::grpc::IndexOptions_CollateMode_CollateNoneMode);
			idx1->set_allocated_options(opts1);

			reindexer::grpc::Index* idx2 = ns1->add_indexesdefinitions();
			idx2->set_name(kAgeField);
			idx2->set_fieldtype("int");
			idx2->set_indextype("tree");
			*(idx2->add_jsonpaths()) = "age";
			idx2->set_expireafter(0);
			reindexer::grpc::IndexOptions* opts2 = idx2->options().New();
			opts2->set_ispk(false);
			opts2->set_collatemode(reindexer::grpc::IndexOptions_CollateMode_CollateNoneMode);
			idx2->set_allocated_options(opts2);

			reindexer::grpc::StorageOptions* storageOpts = ns1->storageoptions().New();
			storageOpts->set_sync(false);
			storageOpts->set_nsname(default_namespace);
			storageOpts->set_enabled(false);
			storageOpts->set_fillcache(false);
			storageOpts->set_slavemode(false);
			storageOpts->set_autorepair(false);
			storageOpts->set_createifmissing(true);
			storageOpts->set_verifychecksums(false);
			storageOpts->set_droponfileformaterror(true);
			ns1->set_allocated_storageoptions(storageOpts);
		}
		addNsRequest.set_allocated_namespace_(ns1);
		grpc::ClientContext addNsCtx;
		status = rx_->AddNamespace(&addNsCtx, addNsRequest, &errResponse);
		ASSERT_TRUE(status.ok()) << status.error_message();
		ASSERT_TRUE(errResponse.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << errResponse.what();

		reindexer::grpc::AddNamespaceRequest addNsRequest2;
		addNsRequest2.set_dbname(kDbName);
		reindexer::grpc::Namespace* ns2 = addNsRequest.namespace_().New();
		{
			ns2->set_dbname(kDbName);
			ns2->set_name(default_namespace + "2");
			ns2->set_istemporary(false);

			reindexer::grpc::Index* idx1 = ns2->add_indexesdefinitions();
			idx1->set_name(kIdField);
			idx1->set_fieldtype("int");
			idx1->set_indextype("hash");
			*(idx1->add_jsonpaths()) = "id";
			idx1->set_expireafter(0);
			reindexer::grpc::IndexOptions* opts1 = idx1->options().New();
			opts1->set_ispk(true);
			opts1->set_collatemode(reindexer::grpc::IndexOptions_CollateMode_CollateNoneMode);
			idx1->set_allocated_options(opts1);

			reindexer::grpc::Index* idx2 = ns2->add_indexesdefinitions();
			idx2->set_name(kPriceField);
			idx2->set_fieldtype("int");
			idx2->set_indextype("tree");
			*(idx2->add_jsonpaths()) = "price";
			idx2->set_expireafter(0);
			reindexer::grpc::IndexOptions* opts2 = idx2->options().New();
			opts2->set_ispk(false);
			opts2->set_collatemode(reindexer::grpc::IndexOptions_CollateMode_CollateNoneMode);
			idx2->set_allocated_options(opts2);

			reindexer::grpc::StorageOptions* storageOpts = ns2->storageoptions().New();
			storageOpts->set_sync(false);
			storageOpts->set_nsname(default_namespace);
			storageOpts->set_enabled(false);
			storageOpts->set_fillcache(false);
			storageOpts->set_slavemode(false);
			storageOpts->set_autorepair(false);
			storageOpts->set_createifmissing(true);
			storageOpts->set_verifychecksums(false);
			storageOpts->set_droponfileformaterror(true);
			ns2->set_allocated_storageoptions(storageOpts);
		}
		addNsRequest2.set_allocated_namespace_(ns2);
		grpc::ClientContext addNs2Ctx;
		status = rx_->AddNamespace(&addNs2Ctx, addNsRequest2, &errResponse);
		ASSERT_TRUE(status.ok()) << status.error_message();
		ASSERT_TRUE(errResponse.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << errResponse.what();
		InsertData();
	}

	void TearDown() {
		srv_.Stop();
		serverThread_.join();
	}

	void InsertData() {
		reindexer::TagsMatcher tm1;
		tm1.name2tag(kIdField, true);
		tm1.name2tag(kAgeField, true);

		reindexer::TagsMatcher tm2;
		tm2.name2tag(kIdField, true);
		tm2.name2tag(kPriceField, true);

		grpc::ClientContext ctx;
		std::unique_ptr<::grpc::ClientReaderWriter<reindexer::grpc::ModifyItemRequest, reindexer::grpc::ErrorResponse>> stream =
			rx_->ModifyItem(&ctx);

		reindexer::WrSerializer wrser;
		for (size_t i = 0; i < 100; ++i) {
			wrser.Reset();
			reindexer::CJsonBuilder cjsonBuilder(wrser);
			cjsonBuilder.Put(tm1.name2tag(kIdField), int(i));
			cjsonBuilder.Put(tm1.name2tag(kAgeField), int(rand() % 60 + 18));
			cjsonBuilder.End();

			reindexer::grpc::ModifyItemRequest modifyRequest;
			modifyRequest.set_dbname(kDbName);
			modifyRequest.set_nsname(default_namespace);
			modifyRequest.set_mode(reindexer::grpc::ModifyMode::INSERT);
			modifyRequest.set_encodingtype(reindexer::grpc::EncodingType::CJSON);
			modifyRequest.set_data(string(wrser.Slice()));
			ASSERT_TRUE(stream->Write(modifyRequest));

			reindexer::grpc::ErrorResponse errResponse;
			ASSERT_TRUE(stream->Read(&errResponse));
			ASSERT_TRUE(errResponse.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << errResponse.what();

			wrser.Reset();
			reindexer::CJsonBuilder cjsonBuilder2(wrser);
			cjsonBuilder2.Put(tm2.name2tag(kIdField), int(i));
			cjsonBuilder2.Put(tm2.name2tag(kPriceField), int(rand() % 999 + 99));
			cjsonBuilder2.End();

			modifyRequest.set_nsname(default_namespace + "2");
			modifyRequest.set_data(string(wrser.Slice()));
			ASSERT_TRUE(stream->Write(modifyRequest));

			reindexer::grpc::ErrorResponse errResponse2;
			ASSERT_TRUE(stream->Read(&errResponse2));
			ASSERT_TRUE(errResponse2.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << errResponse.what();
		}
	}

	using NsType = pair<reindexer::TagsMatcher, reindexer::PayloadTypeImpl>;

	void checkPayloadTypes(reindexer::Serializer& rser, vector<NsType>& types, bool print = false) {
		int ptCount = rser.GetVarUint();
		for (int i = 0; i < ptCount; i++) {
			uint64_t nsid = rser.GetVarUint();
			ASSERT_TRUE(nsid == 0 || nsid == 1) << nsid;
			string nsName = string(rser.GetVString());
			if (print) {
				std::cout << "ns: " << nsName << " [" << nsid << "]" << std::endl;
			}
			uint64_t stateToken = rser.GetVarUint();
			uint64_t version = rser.GetVarUint();
			if (print) {
				std::cout << "version: " << version << "; stateToke: " << stateToken << std::endl;
			}
			reindexer::TagsMatcher tm;
			reindexer::PayloadTypeImpl pt(nsName);
			tm.deserialize(rser, version, stateToken);
			pt.deserialize(rser);
			int tag = 0;
			ASSERT_NO_THROW(tag = tm.name2tag(kIdField));
			ASSERT_TRUE(tag == 1);
			if (nsid == 0) {
				ASSERT_NO_THROW(tag = tm.name2tag(kAgeField));
				ASSERT_TRUE(tag == 2) << tag;
			} else if (nsid == 1) {
				ASSERT_NO_THROW(tag = tm.name2tag(kPriceField));
				ASSERT_TRUE(tag == 2) << tag;
			} else {
				std::abort();
			}
			ASSERT_NO_THROW(pt.FieldByName(kIdField));
			ASSERT_TRUE(pt.Field(1).Name() == kIdField) << pt.Field(1).Name();
			ASSERT_TRUE(pt.Field(1).Type() == KeyValueInt) << pt.Field(1).Type();
			if (nsid == 0) {
				ASSERT_TRUE(pt.Field(2).Name() == kAgeField) << pt.Field(2).Name();
				ASSERT_TRUE(pt.Field(2).Type() == KeyValueInt) << pt.Field(2).Type();
			} else if (nsid == 1) {
				ASSERT_TRUE(pt.Field(2).Name() == kPriceField) << pt.Field(2).Name();
				ASSERT_TRUE(pt.Field(2).Type() == KeyValueInt) << pt.Field(2).Type();
			}
			types.emplace_back(tm, pt);
		}
		std::cout << std::endl;
	}

	void checkCJSON(const NsType& nsTypes, reindexer::string_view& cjson) {
		reindexer::PayloadValue value;
		reindexer::Payload pl(nsTypes.second, value);

		reindexer::WrSerializer wrser;
		reindexer::Serializer rdser(cjson);

		reindexer::CJsonDecoder decoder(const_cast<reindexer::TagsMatcher&>(nsTypes.first));
		Error status = decoder.Decode(&pl, rdser, wrser);
		ASSERT_TRUE(status.ok()) << status.what();
		ASSERT_TRUE(rdser.Eof());
	}

	void checkItem(reindexer::Serializer& rser, reindexer::grpc::OutputFlags* flags, const vector<NsType>& nsTypes, bool joined = false,
				   bool print = false) {
		uint64_t nsId = 0;
		if (flags->withnsid()) {
			nsId = rser.GetVarUint();
			if (print) {
				std::cout << "nsid: " << nsId;
			}
		}
		if (flags->withitemid()) {
			uint64_t rowId = rser.GetVarUint();
			uint64_t lsn = rser.GetVarUint();
			if (print) {
				std::cout << "; rowid: " << rowId;
				std::cout << "; lsn: " << lsn;
			}
			if (!joined) {
				if (lastRowId_ != INT_MAX) {
					ASSERT_TRUE(rowId > lastRowId_) << rowId;
					ASSERT_TRUE(lsn > lastLsn_) << lsn;
				}
				lastRowId_ = rowId;
				lastLsn_ = lsn;
			}
		}
		if (flags->withrank()) {
			uint64_t rank = rser.GetVarUint();
			if (print) {
				std::cout << "; rank: " << rank;
			}
		}
		if (print && (flags->withnsid() || flags->withitemid() || flags->withrank())) {
			std::cout << std::endl;
		}
		reindexer::string_view cjson = rser.GetSlice();
		checkCJSON(nsTypes.at(nsId), cjson);
	}

	void checkCJSONItems(reindexer::Serializer& rser, reindexer::grpc::OutputFlags* flags, bool print = false) {
		lastLsn_ = 0;
		lastRowId_ = INT_MAX;

		vector<NsType> nsTypes;
		checkPayloadTypes(rser, nsTypes, print);
		while (rser.Pos() < rser.Len()) {
			checkItem(rser, flags, nsTypes, false, print);
			if (flags->withjoineditems()) {
				uint64_t joinedFields = rser.GetVarUint();
				if (print) {
					std::cout << "joined fields: " << joinedFields << std::endl;
				}
				for (uint64_t i = 0; i < joinedFields; ++i) {
					uint64_t joinedItems = rser.GetVarUint();
					if (print) {
						std::cout << "items joined: " << joinedItems << std::endl;
					}
					for (uint64_t j = 0; j < joinedItems; ++j) {
						checkItem(rser, flags, nsTypes, true, print);
					}
				}
			}
			if (print) {
				std::cout << std::endl;
			}
		}
	}

private:
	reindexer_server::Server srv_;
	std::thread serverThread_;

protected:
	const string kDbName = "grpcdb";
	const string kIdField = "id";
	const string kAgeField = "age";
	const string kPriceField = "price";
	std::unique_ptr<reindexer::grpc::Reindexer::Stub> rx_;
	uint64_t lastLsn_ = 0, lastRowId_ = INT_MAX;

	const std::string kStoragePath = "/tmp/reindex_grpc_test/";
};

#endif
