#ifdef WITH_GRPC

#include "core/itemimpl.h"
#include "grpcclient_api.h"

TEST_F(GrpcClientApi, SelectCJSON) {
	reindexer::Query q(default_namespace);
	q.InnerJoin(kIdField, kIdField, CondEq, reindexer::Query(default_namespace + "2"));

	reindexer::grpc::SelectSqlRequest request;
	request.set_dbname(kDbName);
	request.set_sql(q.GetSQL());

	reindexer::grpc::OutputFlags *flags = request.flags().New();
	flags->set_encodingtype(reindexer::grpc::EncodingType::CJSON);
	flags->set_withnsid(true);
	flags->set_withrank(true);
	flags->set_withitemid(true);
	flags->set_withjoineditems(true);
	request.set_allocated_flags(flags);

	grpc::ClientContext context;
	std::unique_ptr<grpc::ClientReader<reindexer::grpc::QueryResultsResponse>> reader = rx_->SelectSql(&context, request);

	reindexer::grpc::QueryResultsResponse response;
	while (reader->Read(&response)) {
		reindexer::Serializer rser(response.data());
		checkCJSONItems(rser, flags);
	}
}

TEST_F(GrpcClientApi, SelectJSON) {
	reindexer::Query q(default_namespace);
	q.InnerJoin(kIdField, kIdField, CondEq, reindexer::Query(default_namespace + "2"));

	reindexer::grpc::SelectSqlRequest request;
	request.set_dbname(kDbName);
	request.set_sql(q.GetSQL());

	reindexer::grpc::OutputFlags *flags = request.flags().New();
	flags->set_encodingtype(reindexer::grpc::EncodingType::JSON);
	flags->set_withnsid(true);
	flags->set_withitemid(true);
	flags->set_withjoineditems(true);
	request.set_allocated_flags(flags);

	grpc::ClientContext context;
	std::unique_ptr<grpc::ClientReader<reindexer::grpc::QueryResultsResponse>> reader = rx_->SelectSql(&context, request);

	reindexer::grpc::QueryResultsResponse response;
	while (reader->Read(&response)) {
		gason::JsonParser parser;
		size_t len = 0;
		ASSERT_NO_THROW(parser.Parse(string_view(response.data().c_str(), response.data().length()), &len));
		ASSERT_TRUE(len > 0);
	}
}

#endif
