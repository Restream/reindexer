#pragma once

#ifdef WITH_GRPC
#include "reindexer.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <thread>
#include "core/cjson/cjsonbuilder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/payload/payloadiface.h"
#include "reindexer_api.h"
#include "server/server.h"
#include "tools/fsops.h"
#include "yaml-cpp/yaml.h"

class [[nodiscard]] GrpcClientApi : public ReindexerApi {
public:
	void SetUp() {
		std::ignore = reindexer::fs::RmDirAll(kStoragePath);
		YAML::Node y;
		y["storage"]["path"] = kStoragePath;
		y["logger"]["loglevel"] = "none";
		y["logger"]["rpclog"] = "none";
		y["logger"]["serverlog"] = "none";
		y["logger"]["corelog"] = "none";
		y["net"]["httpaddr"] = "0.0.0.0:5554";
		y["net"]["rpcaddr"] = "0.0.0.0:4443";
		y["net"]["grpc"] = true;

		auto err = srv_.InitFromYAML(YAML::Dump(y));
		EXPECT_TRUE(err.ok()) << err.what();

		serverThread_ = std::thread([this]() {
			auto res = this->srv_.Start();
			if (res != EXIT_SUCCESS) {
				std::cerr << "Exit code: " << res << std::endl;
			}
			assertrx(res == EXIT_SUCCESS);
		});
		while (!srv_.IsReady()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}

		for (int i = 0; i < 10; i++) {
			auto channel = grpc::CreateChannel("127.0.0.1:16534", grpc::InsecureChannelCredentials());
			if (channel->WaitForConnected(reindexer::system_clock_w::now() + std::chrono::seconds(1))) {
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
		auto _ = tm1.name2tag(kIdField, reindexer::CanAddField_True);
		_ = tm1.name2tag(kAgeField, reindexer::CanAddField_True);

		reindexer::TagsMatcher tm2;
		_ = tm2.name2tag(kIdField, reindexer::CanAddField_True);
		_ = tm2.name2tag(kPriceField, reindexer::CanAddField_True);

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
			modifyRequest.set_data(std::string(wrser.Slice()));
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
			modifyRequest.set_data(std::string(wrser.Slice()));
			ASSERT_TRUE(stream->Write(modifyRequest));

			reindexer::grpc::ErrorResponse errResponse2;
			ASSERT_TRUE(stream->Read(&errResponse2));
			ASSERT_TRUE(errResponse2.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << errResponse.what();
		}
	}

	using NsType = std::pair<reindexer::TagsMatcher, reindexer::PayloadTypeImpl>;

	void checkPayloadTypes(reindexer::Serializer& rser, std::vector<NsType>& types, bool print = false) {
		using namespace reindexer;
		int ptCount = rser.GetVarUInt();
		for (int i = 0; i < ptCount; i++) {
			uint64_t nsid = rser.GetVarUInt();
			ASSERT_TRUE(nsid == 0 || nsid == 1) << nsid;
			std::string nsName = std::string(rser.GetVString());
			if (print) {
				std::cout << "ns: " << nsName << " [" << nsid << "]" << std::endl;
			}
			uint64_t stateToken = rser.GetVarUInt();
			uint64_t version = rser.GetVarUInt();
			if (print) {
				std::cout << "version: " << version << "; stateToke: " << stateToken << std::endl;
			}
			TagsMatcher tm;
			PayloadTypeImpl pt(nsName);
			tm.deserialize(rser, version, stateToken);
			pt.deserialize(rser);
			auto tag = TagName::Empty();
			ASSERT_NO_THROW(tag = tm.name2tag(kIdField));
			ASSERT_TRUE(tag == 1_Tag);
			if (nsid == 0) {
				ASSERT_NO_THROW(tag = tm.name2tag(kAgeField));
				ASSERT_EQ(tag, 2_Tag) << tag.AsNumber();
			} else if (nsid == 1) {
				ASSERT_NO_THROW(tag = tm.name2tag(kPriceField));
				ASSERT_EQ(tag, 2_Tag) << tag.AsNumber();
			} else {
				std::abort();
			}
			ASSERT_NO_THROW([[maybe_unused]] auto fldId = pt.FieldByName(kIdField));
			ASSERT_TRUE(pt.Field(1).Name() == kIdField) << pt.Field(1).Name();
			ASSERT_TRUE(pt.Field(1).Type().Is<KeyValueType::Int>()) << KeyValueType{pt.Field(1).Type()}.Name();
			if (nsid == 0) {
				ASSERT_TRUE(pt.Field(2).Name() == kAgeField) << pt.Field(2).Name();
				ASSERT_TRUE(pt.Field(2).Type().Is<KeyValueType::Int>()) << KeyValueType{pt.Field(2).Type()}.Name();
			} else if (nsid == 1) {
				ASSERT_TRUE(pt.Field(2).Name() == kPriceField) << pt.Field(2).Name();
				ASSERT_TRUE(pt.Field(2).Type().Is<KeyValueType::Int>()) << KeyValueType{pt.Field(2).Type()}.Name();
			}
			types.emplace_back(tm, pt);
		}
		std::cout << std::endl;
	}

	void checkCJSON(const NsType& nsTypes, std::string_view& cjson) {
		reindexer::PayloadValue value;
		reindexer::Payload pl(nsTypes.second, value);

		reindexer::WrSerializer wrser;
		reindexer::Serializer rdser(cjson);

		reindexer::h_vector<reindexer::key_string, 16> storage;
		reindexer::CJsonDecoder decoder(const_cast<reindexer::TagsMatcher&>(nsTypes.first), storage);
		reindexer::FloatVectorsHolderVector floatVectorsHolder;
		ASSERT_NO_THROW(decoder.Decode<>(pl, rdser, wrser, floatVectorsHolder, reindexer::CJsonDecoder::DefaultFilter{nullptr}));
		ASSERT_TRUE(rdser.Eof());
	}

	void checkItem(reindexer::Serializer& rser, reindexer::grpc::OutputFlags* flags, const std::vector<NsType>& nsTypes,
				   bool joined = false, bool print = false) {
		uint64_t nsId = 0;
		if (flags->withnsid()) {
			nsId = rser.GetVarUInt();
			if (print) {
				std::cout << "nsid: " << nsId;
			}
		}
		if (flags->withitemid()) {
			uint64_t rowId = rser.GetVarUInt();
			uint64_t lsn = rser.GetVarUInt();
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
			const auto rank = rser.GetFloat();
			if (print) {
				std::cout << "; rank: " << rank;
			}
		}
		if (print && (flags->withnsid() || flags->withitemid() || flags->withrank())) {
			std::cout << std::endl;
		}
		std::string_view cjson = rser.GetSlice();
		checkCJSON(nsTypes.at(nsId), cjson);
	}

	void checkCJSONItems(reindexer::Serializer& rser, reindexer::grpc::OutputFlags* flags, bool print = false) {
		lastLsn_ = 0;
		lastRowId_ = INT_MAX;

		std::vector<NsType> nsTypes;
		checkPayloadTypes(rser, nsTypes, print);
		while (rser.Pos() < rser.Len()) {
			checkItem(rser, flags, nsTypes, false, print);
			if (flags->withjoineditems()) {
				uint64_t joinedFields = rser.GetVarUInt();
				if (print) {
					std::cout << "joined fields: " << joinedFields << std::endl;
				}
				for (uint64_t i = 0; i < joinedFields; ++i) {
					uint64_t joinedItems = rser.GetVarUInt();
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
	const std::string kDbName = "grpcdb";
	const std::string kIdField = "id";
	const std::string kAgeField = "age";
	const std::string kPriceField = "price";
	std::unique_ptr<reindexer::grpc::Reindexer::Stub> rx_;
	uint64_t lastLsn_ = 0, lastRowId_ = INT_MAX;

	const std::string kStoragePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex_grpc_test");
};

#endif
