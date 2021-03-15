#pragma once

#ifdef WITH_GRPC
#include "reindexer.grpc.pb.h"

#include <unordered_map>
#include "core/transaction.h"
#include "net/ev/ev.h"

namespace reindexer_server {
class DBManager;
}

namespace reindexer {

class Reindexer;

namespace grpc {

class ReindexerService : public Reindexer::Service {
public:
	using Base = Reindexer::Service;
	ReindexerService(reindexer_server::DBManager& dbMgr, std::chrono::seconds txIdleTimeout, reindexer::net::ev::dynamic_loop& loop);
	ReindexerService(const ReindexerService&) = delete;
	ReindexerService(ReindexerService&&) = delete;
	ReindexerService& operator=(const ReindexerService&) = delete;
	ReindexerService& operator=(ReindexerService&&) = delete;

	::grpc::Status Connect(::grpc::ServerContext* context, const ConnectRequest* request, ErrorResponse* response) override;
	::grpc::Status CreateDatabase(::grpc::ServerContext* context, const CreateDatabaseRequest* request, ErrorResponse* response) override;
	::grpc::Status OpenNamespace(::grpc::ServerContext* context, const OpenNamespaceRequest* request, ErrorResponse* response) override;
	::grpc::Status AddNamespace(::grpc::ServerContext* context, const AddNamespaceRequest* request, ErrorResponse* response) override;
	::grpc::Status CloseNamespace(::grpc::ServerContext* context, const CloseNamespaceRequest* request, ErrorResponse* response) override;
	::grpc::Status DropNamespace(::grpc::ServerContext* context, const DropNamespaceRequest* request, ErrorResponse* response) override;
	::grpc::Status TruncateNamespace(::grpc::ServerContext* context, const TruncateNamespaceRequest* request,
									 ErrorResponse* response) override;
	::grpc::Status AddIndex(::grpc::ServerContext* context, const AddIndexRequest* request, ErrorResponse* response) override;
	::grpc::Status UpdateIndex(::grpc::ServerContext* context, const UpdateIndexRequest* request, ErrorResponse* response) override;
	::grpc::Status DropIndex(::grpc::ServerContext* context, const DropIndexRequest* request, ErrorResponse* response) override;
	::grpc::Status SetSchema(::grpc::ServerContext* context, const SetSchemaRequest* request, ErrorResponse* response) override;
	::grpc::Status GetProtobufSchema(::grpc::ServerContext* context, const GetProtobufSchemaRequest* request,
									 ProtobufSchemaResponse* response) override;
	::grpc::Status EnumNamespaces(::grpc::ServerContext* context, const EnumNamespacesRequest* request,
								  EnumNamespacesResponse* response) override;
	::grpc::Status EnumDatabases(::grpc::ServerContext* context, const EnumDatabasesRequest* request,
								 EnumDatabasesResponse* response) override;
	::grpc::Status ModifyItem(::grpc::ServerContext* context,
							  ::grpc::ServerReaderWriter<ErrorResponse, ModifyItemRequest>* stream) override;
	::grpc::Status SelectSql(::grpc::ServerContext* context, const SelectSqlRequest* request,
							 ::grpc::ServerWriter<QueryResultsResponse>* writer) override;
	::grpc::Status Select(::grpc::ServerContext* context, const SelectRequest* request,
						  ::grpc::ServerWriter<QueryResultsResponse>* writer) override;
	::grpc::Status Update(::grpc::ServerContext* context, const UpdateRequest* request,
						  ::grpc::ServerWriter<QueryResultsResponse>* writer) override;
	::grpc::Status Delete(::grpc::ServerContext* context, const DeleteRequest* request,
						  ::grpc::ServerWriter<QueryResultsResponse>* writer) override;
	::grpc::Status GetMeta(::grpc::ServerContext* context, const GetMetaRequest* request, MetadataResponse* response) override;
	::grpc::Status PutMeta(::grpc::ServerContext* context, const PutMetaRequest* request, ErrorResponse* response) override;
	::grpc::Status EnumMeta(::grpc::ServerContext* context, const EnumMetaRequest* request, MetadataKeysResponse* response) override;
	::grpc::Status BeginTransaction(::grpc::ServerContext* context, const BeginTransactionRequest* request,
									TransactionIdResponse* response) override;
	::grpc::Status AddTxItem(::grpc::ServerContext* context, ::grpc::ServerReaderWriter<ErrorResponse, AddTxItemRequest>* stream) override;
	::grpc::Status CommitTransaction(::grpc::ServerContext* context, const CommitTransactionRequest* request,
									 ErrorResponse* response) override;
	::grpc::Status RollbackTransaction(::grpc::ServerContext* context, const RollbackTransactionRequest* request,
									   ErrorResponse* response) override;

private:
	struct TxData {
		std::shared_ptr<Transaction> tx;
		std::chrono::time_point<std::chrono::steady_clock> txDeadline;
		string dbName, nsName;
	};

	static ::grpc::Status buildQueryResults(const reindexer::QueryResults& qr, ::grpc::ServerWriter<QueryResultsResponse>* writer,
											const OutputFlags& opts);
	static Error buildQrItems(WrSerializer& wrser, const reindexer::QueryResults& qr, const OutputFlags& opts);
	static Error buildAggregation(WrSerializer& wrser, const reindexer::QueryResults& qr, const OutputFlags& opts);

	Error getDB(const std::string& dbName, int userRole, reindexer::Reindexer** rx);
	void removeExpiredTxCb(reindexer::net::ev::periodic&, int);

	static Error packCJSONItem(WrSerializer& wrser, reindexer::QueryResults::Iterator& it, const OutputFlags& opts);
	static Error packPayloadTypes(WrSerializer& wrser, const reindexer::QueryResults& qr);

	Error executeQuery(const std::string& dbName, const Query& q, QueryType type, reindexer::QueryResults& qr);
	Error getTx(uint64_t id, TxData& txData);

	reindexer_server::DBManager& dbMgr_;
	std::mutex m_;
	std::unordered_map<uint64_t, TxData> transactions_;
	uint64_t txID_ = {0};
	const std::chrono::seconds txIdleTimeout_;
	reindexer::net::ev::timer expirationChecker_;
};

}  // namespace grpc
}  // namespace reindexer

#endif
