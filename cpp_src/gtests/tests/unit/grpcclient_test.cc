#if defined(WITH_GRPC)

#include "core/itemimpl.h"
#include "grpcclient_api.h"

TEST_F(GrpcClientApi, SelectCJSON) {
	reindexer::Query q(default_namespace);
	q.InnerJoin(kIdField, kIdField, CondEq, reindexer::Query(default_namespace + "2"));

	reindexer::grpc::SelectSqlRequest request;
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
	std::unique_ptr<grpc::ClientReader<reindexer::grpc::QueryResultsResponse>> reader = rx_->SelectSql(&context, request);

	reindexer::grpc::QueryResultsResponse response;
	while (reader->Read(&response)) {
		reindexer::Serializer rser(response.data());
		checkCJSONItems(rser, flags);
	}
}

// Perform Select with GRPC-service with
// JSON as output format
TEST_F(GrpcClientApi, SelectJSON) {
	// Build query with join, distinct and simple Where condition
	reindexer::Query q(default_namespace);
	q.Select({kIdField.c_str(), kAgeField.c_str()});
	q.Distinct(kAgeField);
	q.InnerJoin(kIdField, kIdField, CondEq, reindexer::Query(default_namespace + "2"));

	// Set input data for GRPC query
	reindexer::grpc::SelectSqlRequest request;
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
	std::unique_ptr<grpc::ClientReader<reindexer::grpc::QueryResultsResponse>> reader = rx_->SelectSql(&context, request);

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
			const auto& v(elem.value);
			std::string_view name(elem.key);
			if (name == "items") {
				ASSERT_TRUE(v.getTag() == gason::JSON_ARRAY);
				for (const auto& element : v) {
					auto& object = element.value;
					ASSERT_TRUE(object.getTag() == gason::JSON_OBJECT);
					for (auto field : object) {
						name = std::string_view(field.key);
						const auto& fieldValue(field.value);
						if (name == "id") {
							ASSERT_TRUE(fieldValue.getTag() == gason::JSON_NUMBER);
						} else if (name == "joined_test_namespace2") {
							ASSERT_TRUE(fieldValue.getTag() == gason::JSON_ARRAY);
							for (const auto& item : fieldValue) {
								ASSERT_TRUE(item.value.getTag() == gason::JSON_OBJECT);
								for (const auto& joinedField : item.value) {
									name = std::string_view(joinedField.key);
									const auto& joinedFieldValue(joinedField.value);
									if (name == "id") {
										ASSERT_TRUE(joinedFieldValue.getTag() == gason::JSON_NUMBER);
									} else if (name == "price") {
										ASSERT_TRUE(joinedFieldValue.getTag() == gason::JSON_NUMBER);
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
				ASSERT_TRUE(v.getTag() == gason::JSON_ARRAY);
				for (const auto& element : v) {
					auto& object = element.value;
					ASSERT_TRUE(object.getTag() == gason::JSON_OBJECT);
					for (const auto& field : object) {
						name = std::string_view(field.key);
						const auto& fieldValue(field.value);
						if (name == "type") {
							ASSERT_TRUE(fieldValue.getTag() == gason::JSON_STRING);
							ASSERT_TRUE(fieldValue.toString() == "distinct");
						} else if (name == "distincts") {
							ASSERT_TRUE(fieldValue.getTag() == gason::JSON_ARRAY);
							for (const auto& items : fieldValue) {
								ASSERT_TRUE(items.value.getTag() == gason::JSON_STRING);
							}
						} else if (name == "fields") {
							ASSERT_TRUE(fieldValue.getTag() == gason::JSON_ARRAY);
							for (const auto& items : fieldValue) {
								ASSERT_TRUE(items.value.getTag() == gason::JSON_STRING);
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

#endif
