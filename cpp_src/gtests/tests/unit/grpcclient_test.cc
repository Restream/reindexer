#if defined(WITH_GRPC)

#include "grpcclient_api.h"
#include "vendor/gason/gason.h"

#include <chrono>
#include <thread>
#include <tuple>

namespace reindexer_tests {

using reindexer::Query;

TEST_F(GrpcClientApi, SelectCJSON) try {
	const auto q = Query(default_namespace).InnerJoin(Query(default_namespace + "2"), kIdField, CondEq, kIdField);

	reindexer::grpc::SqlRequest request;
	request.set_dbname(kDbName);
	request.set_sql(q.GetSQL());

	reindexer::grpc::OutputFlags* flags = request.flags().New();
	flags->set_encodingtype(reindexer::grpc::EncodingType::CJSON);
	flags->set_withnsid(true);
	flags->set_withrank(true);
	flags->set_withitemid(true);
	flags->set_withjoineditems(true);
	request.set_allocated_flags(flags);

	grpc::ClientContext context;
	std::unique_ptr<grpc::ClientReader<reindexer::grpc::QueryResultsResponse>> reader = rx_->ExecSql(&context, request);

	reindexer::grpc::QueryResultsResponse response;
	while (reader->Read(&response)) {
		reindexer::Serializer rser(response.data());
		checkCJSONItems(rser, flags);
	}
} catch (const Error& err) {
	ASSERT_TRUE(false) << err.what();
} catch (const std::exception& err) {
	ASSERT_TRUE(false) << err.what();
} catch (...) {
	ASSERT_TRUE(false) << "Unknown exception";
}

// Perform Select with GRPC-service with
// JSON as output format
TEST_F(GrpcClientApi, SelectJSON) {
	// Build query with join, distinct and simple Where condition
	const auto q = Query(default_namespace)
					   .Select(kIdField, kAgeField)
					   .Distinct(kAgeField)
					   .InnerJoin(Query(default_namespace + "2"), kIdField, CondEq, kIdField);

	// Set input data for GRPC query
	reindexer::grpc::SqlRequest request;
	request.set_dbname(kDbName);
	request.set_sql(q.GetSQL());

	reindexer::grpc::OutputFlags* flags = request.flags().New();
	flags->set_encodingtype(reindexer::grpc::EncodingType::JSON);
	flags->set_withnsid(true);
	flags->set_withitemid(true);
	flags->set_withjoineditems(true);
	request.set_allocated_flags(flags);

	// Execute GRPC query
	grpc::ClientContext context;
	std::unique_ptr<grpc::ClientReader<reindexer::grpc::QueryResultsResponse>> reader = rx_->ExecSql(&context, request);

	// Read answer and make sure output JSON has a correct format
	reindexer::grpc::QueryResultsResponse response;
	while (reader->Read(&response)) {
		std::string_view json(response.data().c_str(), response.data().length());
		gason::JsonNode root;
		gason::JsonParser parser;
		size_t len = 0;
		ASSERT_NO_THROW(root = parser.Parse(json, &len));
		ASSERT_TRUE(len > 0);

		for (const auto& elem : root) {
			std::string_view name(elem.key);
			if (name == "items") {
				ASSERT_TRUE(elem.isArray());
				for (const auto& element : elem.value) {
					ASSERT_TRUE(element.isObject());
					for (auto field : element.value) {
						name = std::string_view(field.key);
						const auto& fieldValue(field.value);
						if (name == "id" || name == "age") {
							ASSERT_TRUE(fieldValue.getTag() == gason::JsonTag::NUMBER);
						} else if (name == "joined_test_namespace2") {
							ASSERT_TRUE(field.isArray());
							for (const auto& item : fieldValue) {
								ASSERT_TRUE(item.isObject());
								for (const auto& joinedField : item.value) {
									name = std::string_view(joinedField.key);
									const auto& joinedFieldValue(joinedField.value);
									if (name == "id" || name == "price") {
										ASSERT_TRUE(joinedFieldValue.getTag() == gason::JsonTag::NUMBER);
									} else {
										ASSERT_TRUE(false) << "Wrong JSON field: " << name;
									}
								}
							}
						} else {
							ASSERT_TRUE(false) << "Wrong JSON field: " << name;
						}
					}
				}
			} else if (name == "aggregations") {
				ASSERT_TRUE(elem.isArray());
				for (const auto& element : elem.value) {
					ASSERT_TRUE(element.isObject());
					for (const auto& field : element.value) {
						name = std::string_view(field.key);
						const auto& fieldValue(field.value);
						if (name == "type") {
							ASSERT_TRUE(fieldValue.getTag() == gason::JsonTag::STRING);
							ASSERT_TRUE(fieldValue.toString() == "distinct");
						} else if (name == "distincts") {
							ASSERT_TRUE(field.isArray());
							for (const auto& items : fieldValue) {
								ASSERT_TRUE(items.value.getTag() == gason::JsonTag::STRING);
							}
						} else if (name == "fields") {
							ASSERT_TRUE(field.isArray());
							for (const auto& items : fieldValue) {
								ASSERT_TRUE(items.value.getTag() == gason::JsonTag::STRING);
								ASSERT_TRUE(items.value.toString() == "age");
							}
						}
					}
				}
			} else {
				ASSERT_TRUE(false) << "Wrong JSON field: " << name;
			}
		}
	}
}

// Idle timeout is restarted by each transaction call. Expiration is checked once a second.
class [[nodiscard]] GrpcTxIdleApi : public ::testing::Test {
public:
	static constexpr std::chrono::seconds kTxIdleTimeout{5};
	static constexpr std::chrono::seconds kCheckerPeriod{1};
	static constexpr std::chrono::seconds kActivityGap{2};

	void SetUp() override {
		std::ignore = reindexer::fs::RmDirAll(kStoragePath);
		YAML::Node y;
		y["storage"]["path"] = kStoragePath;
		y["logger"]["loglevel"] = "none";
		y["logger"]["rpclog"] = "none";
		y["logger"]["serverlog"] = "none";
		y["logger"]["corelog"] = "none";
		y["logger"]["grpclog"] = "none";
		y["net"]["httpaddr"] = "0.0.0.0:5558";
		y["net"]["rpcaddr"] = "0.0.0.0:4448";
		y["net"]["grpc"] = true;
		y["net"]["grpcaddr"] = "0.0.0.0:16538";
		y["net"]["tx_idle_timeout"] = int(kTxIdleTimeout.count());

		auto err = srv_.InitFromYAML(YAML::Dump(y));
		ASSERT_TRUE(err.ok()) << err.what();

		serverThread_ = std::thread([this] {
			const int res = this->srv_.Start();
			assertrx(res == EXIT_SUCCESS);
		});
		const auto readyDeadline = reindexer::steady_clock_w::now() + std::chrono::seconds(30);
		while (!srv_.IsReady() && reindexer::steady_clock_w::now() < readyDeadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
		ASSERT_TRUE(srv_.IsReady());

		for (int i = 0; i < 10; ++i) {
			auto channel = grpc::CreateChannel("127.0.0.1:16538", grpc::InsecureChannelCredentials());
			if (channel->WaitForConnected(reindexer::system_clock_w::now() + std::chrono::seconds(1))) {
				rx_ = reindexer::grpc::Reindexer::NewStub(channel);
				break;
			}
		}
		ASSERT_TRUE(rx_);

		grpc::ClientContext createDbCtx;
		reindexer::grpc::CreateDatabaseRequest createDbRequest;
		createDbRequest.set_dbname(kDbName);
		reindexer::grpc::ErrorResponse errResponse;
		grpc::Status status = rx_->CreateDatabase(&createDbCtx, createDbRequest, &errResponse);
		ASSERT_TRUE(status.ok()) << status.error_message();
		ASSERT_TRUE(errResponse.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << errResponse.what();

		reindexer::grpc::AddNamespaceRequest addNsRequest;
		addNsRequest.set_dbname(kDbName);
		reindexer::grpc::Namespace* ns = addNsRequest.namespace_().New();
		ns->set_dbname(kDbName);
		ns->set_name(kNsName);
		reindexer::grpc::Index* idx = ns->add_indexesdefinitions();
		idx->set_name("id");
		idx->set_fieldtype("int");
		idx->set_indextype("hash");
		*(idx->add_jsonpaths()) = "id";
		idx->set_expireafter(0);
		reindexer::grpc::IndexOptions* opts = idx->options().New();
		opts->set_ispk(true);
		opts->set_collatemode(reindexer::grpc::IndexOptions_CollateMode_CollateNoneMode);
		idx->set_allocated_options(opts);
		reindexer::grpc::StorageOptions* storageOpts = ns->storageoptions().New();
		storageOpts->set_enabled(false);
		storageOpts->set_nsname(kNsName);
		ns->set_allocated_storageoptions(storageOpts);
		addNsRequest.set_allocated_namespace_(ns);

		grpc::ClientContext addNsCtx;
		status = rx_->AddNamespace(&addNsCtx, addNsRequest, &errResponse);
		ASSERT_TRUE(status.ok()) << status.error_message();
		ASSERT_TRUE(errResponse.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << errResponse.what();
	}

	void TearDown() override {
		srv_.Stop();
		if (serverThread_.joinable()) {
			serverThread_.join();
		}
	}

	int64_t BeginTx() {
		grpc::ClientContext ctx;
		reindexer::grpc::BeginTransactionRequest request;
		request.set_dbname(kDbName);
		request.set_nsname(kNsName);
		reindexer::grpc::TransactionIdResponse response;
		const grpc::Status status = rx_->BeginTransaction(&ctx, request, &response);
		EXPECT_TRUE(status.ok()) << status.error_message();
		EXPECT_TRUE(response.status().code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << response.status().what();
		return response.id();
	}

	reindexer::grpc::ErrorResponse AddTxItem(int64_t txId, int id) {
		grpc::ClientContext ctx;
		auto stream = rx_->AddTxItem(&ctx);
		reindexer::grpc::AddTxItemRequest request;
		request.set_id(txId);
		request.set_mode(reindexer::grpc::ModifyMode::INSERT);
		request.set_encodingtype(reindexer::grpc::EncodingType::JSON);
		request.set_data("{\"id\":" + std::to_string(id) + "}");
		reindexer::grpc::ErrorResponse response;
		if (!stream->Write(request)) {
			ADD_FAILURE() << "AddTxItem write failed";
			return response;
		}
		if (!stream->Read(&response)) {
			ADD_FAILURE() << "AddTxItem read failed";
			return response;
		}
		EXPECT_TRUE(stream->WritesDone());
		const grpc::Status status = stream->Finish();
		EXPECT_TRUE(status.ok()) << status.error_message();
		return response;
	}

	reindexer::grpc::ErrorResponse Rollback(int64_t txId) {
		grpc::ClientContext ctx;
		reindexer::grpc::RollbackTransactionRequest request;
		request.set_id(txId);
		reindexer::grpc::ErrorResponse response;
		const grpc::Status status = rx_->RollbackTransaction(&ctx, request, &response);
		EXPECT_TRUE(status.ok()) << status.error_message();
		return response;
	}

	reindexer::grpc::ErrorResponse Commit(int64_t txId) {
		grpc::ClientContext ctx;
		reindexer::grpc::CommitTransactionRequest request;
		request.set_id(txId);
		reindexer::grpc::ErrorResponse response;
		const grpc::Status status = rx_->CommitTransaction(&ctx, request, &response);
		EXPECT_TRUE(status.ok()) << status.error_message();
		return response;
	}

private:
	reindexer_server::Server srv_;
	std::thread serverThread_;
	std::unique_ptr<reindexer::grpc::Reindexer::Stub> rx_;
	const std::string kDbName = "grpc_tx_idle_db";
	const std::string kNsName = "grpc_tx_idle_ns";
	const std::string kStoragePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex_grpc_tx_idle_test");
};

TEST_F(GrpcTxIdleApi, ExpiresWhenIdle) {
	const int64_t txId = BeginTx();
	std::this_thread::sleep_for(kTxIdleTimeout + kCheckerPeriod + std::chrono::seconds(1));

	const auto response = Rollback(txId);
	ASSERT_TRUE(response.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeParams) << response.what();
	ASSERT_EQ(response.what(), "No such transaction ID or transaction has expired");
}

TEST_F(GrpcTxIdleApi, ModifyRestartsIdleTimeout) {
	const int64_t txId = BeginTx();
	// Each step is shorter than the idle timeout, but the whole sequence is longer than timeout plus the checker period.
	// Without restarting the deadline on AddTxItem, rollback would observe an expired transaction.
	for (int id = 1; id <= 4; ++id) {
		const auto added = AddTxItem(txId, id);
		ASSERT_TRUE(added.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << added.what();
		std::this_thread::sleep_for(kActivityGap);
	}

	const auto response = Rollback(txId);
	ASSERT_TRUE(response.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << response.what();
}

TEST_F(GrpcTxIdleApi, ExpiresWhenIdleAfterModify) {
	const int64_t txId = BeginTx();
	const auto added = AddTxItem(txId, 1);
	ASSERT_TRUE(added.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << added.what();
	std::this_thread::sleep_for(kTxIdleTimeout + kCheckerPeriod + std::chrono::seconds(1));

	const auto response = Rollback(txId);
	ASSERT_TRUE(response.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeParams) << response.what();
	ASSERT_EQ(response.what(), "No such transaction ID or transaction has expired");
}

TEST_F(GrpcTxIdleApi, ConcurrentCommitAndRollbackFinalizeOnce) {
	constexpr int kIterations = 1000;
	for (int i = 0; i < kIterations; ++i) {
		const int64_t txId = BeginTx();
		const auto added = AddTxItem(txId, i);
		ASSERT_TRUE(added.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK) << added.what();

		reindexer::grpc::ErrorResponse committed, rolledBack;
		std::thread commitThread([&] { committed = Commit(txId); });
		std::thread rollbackThread([&] { rolledBack = Rollback(txId); });
		commitThread.join();
		rollbackThread.join();

		const bool commitOk = committed.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK;
		const bool rollbackOk = rolledBack.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeOK;
		ASSERT_NE(commitOk, rollbackOk) << "commit: " << committed.what() << "; rollback: " << rolledBack.what();
		const auto& loser = commitOk ? rolledBack : committed;
		ASSERT_TRUE(loser.code() == reindexer::grpc::ErrorResponse_ErrorCode_errCodeParams) << loser.what();
		ASSERT_EQ(loser.what(), "No such transaction ID or transaction has expired");
	}
}

}  // namespace reindexer_tests

#endif
