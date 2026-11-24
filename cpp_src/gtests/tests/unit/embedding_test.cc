#include "gtests/tests/fixtures/embedding_test.h"
#include "core/embedding/embedderscache.h"
#include "core/embedding/protocol/iembed_protocol.h"
#include "core/system_ns_names.h"
#include "gtests/tools.h"
#include "tools/errors.h"

namespace reindexer_tests {

using reindexer::IndexOpts;
using reindexer::FloatVectorIndexOpts;
using namespace std::string_view_literals;
static const std::string kFieldNameId{"id"};

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbedding) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ]
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTBEGIN(bugprone-unchecked-optional-access)
	const auto& embedOpts = embedding.value();
	const auto& embedder = embedOpts.upsertEmbedder.value();
	// NOLINTEND(bugprone-unchecked-optional-access)
	ASSERT_TRUE(embedder.strategy == FloatVectorIndexOpts::EmbedderOpts::Strategy::Always);
	ASSERT_TRUE(!embedOpts.queryEmbedder.has_value());
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingQuery) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ],
				"embedding_strategy": "strict"
			},
			"query_embedder": {
				"URL": "http://127.0.0.1:7778/embedder",
				"cache_tag": "QueryEmbedder"
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTBEGIN(bugprone-unchecked-optional-access)
	const auto& embedOpts = embedding.value();
	const auto& embedder = embedOpts.upsertEmbedder.value();
	// NOLINTEND(bugprone-unchecked-optional-access)
	ASSERT_TRUE(embedder.strategy == FloatVectorIndexOpts::EmbedderOpts::Strategy::Strict);
	ASSERT_TRUE(embedOpts.queryEmbedder.has_value());
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingQueryOnly) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"query_embedder": {
				"URL": "http://127.0.0.1:7778/embedder",
				"cache_tag": "QueryEmbedder"
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
	const auto& embedOpts = embedding.value();
	ASSERT_TRUE(!embedOpts.upsertEmbedder.has_value());
	ASSERT_TRUE(embedOpts.queryEmbedder.has_value());
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbeddingEmpty) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(), "Configuration 'embedding' must contain object 'upsert_embedder' or 'query_embedder'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbedderPool) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ],
				"pool": {}
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTBEGIN(bugprone-unchecked-optional-access)
	const auto& embedder = embedding.value().upsertEmbedder;
	ASSERT_TRUE(embedder.value().strategy == FloatVectorIndexOpts::EmbedderOpts::Strategy::Always);
	// NOLINTEND(bugprone-unchecked-optional-access)
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbedderPoolFull) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ],
				"pool": {
					"connections": 10,
					"connect_timeout_ms": 100,
					"read_timeout_ms": 1000,
					"write_timeout_ms": 1000
				}
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbedderCircuitBreaker) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ],
				"pool": {
					"connections": 10,
					"circuit_breaker": {
						"threshold": 8,
						"threshold_timeout_ms": 12000,
						"cooldown_ms": 3000
					}
				}
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
	const auto& cb = embedding.value().upsertEmbedder.value().pool.circuit_breaker;
	EXPECT_EQ(cb.threshold, 8);
	EXPECT_EQ(cb.threshold_timeout_ms, 12000);
	EXPECT_EQ(cb.cooldown_ms, 3000);
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderCircuitBreakerThreshold) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"fields": [ "idx1" ],
				"pool": {
					"circuit_breaker": {
						"threshold": 256
					}
				}
			}
		}
	}
}
)json"sv);
	ASSERT_FALSE(indexDef);
	ASSERT_STREQ(indexDef.error().what(),
				 "Configuration 'embedding:upsert_embedder:pool:circuit_breaker:threshold' should not be more than 255");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithOpenAIProtocol) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"query_embedder": {
				"URL": "http://127.0.0.1:8080/v1/embeddings",
				"protocol": {
					"type": "openai",
					"model": "local"
				}
			},
			"upsert_embedder": {
				"URL": "http://127.0.0.1:8080/v1/embeddings",
				"protocol": {
					"type": "openai",
					"model": "local"
				},
				"fields": [ "title", "body" ]
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTBEGIN(bugprone-unchecked-optional-access)
	const auto& query = embedding.value().queryEmbedder.value();
	const auto& upsert = embedding.value().upsertEmbedder.value();
	// NOLINTEND(bugprone-unchecked-optional-access)
	EXPECT_EQ(query.protocol, FloatVectorIndexOpts::EmbedderOpts::Protocol::OpenAI);
	EXPECT_EQ(query.model, "local");
	EXPECT_EQ(query.endpointUrl, "http://127.0.0.1:8080/v1/embeddings");
	EXPECT_EQ(upsert.protocol, FloatVectorIndexOpts::EmbedderOpts::Protocol::OpenAI);
	EXPECT_EQ(upsert.model, "local");
	ASSERT_EQ(upsert.fields.size(), 2);
	EXPECT_EQ(upsert.fields[0], "title");
	EXPECT_EQ(upsert.fields[1], "body");
	EXPECT_EQ(upsert.fieldsFormat, FloatVectorIndexOpts::EmbedderOpts::FieldsFormat::Stringify);
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithOpenAIFieldsFormatStringify) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:8080/v1/embeddings",
				"protocol": {
					"type": "openai",
					"model": "local",
					"fields_format": "stringify"
				},
				"fields": [ "title", "body" ]
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
	const auto& upsert = embedding.value().upsertEmbedder.value();
	EXPECT_EQ(upsert.fieldsFormat, FloatVectorIndexOpts::EmbedderOpts::FieldsFormat::Stringify);
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithOpenAIFieldsFormatJoin) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:8080/v1/embeddings",
				"protocol": {
					"type": "openai",
					"model": "local",
					"fields_format": "join"
				},
				"fields": [ "title", "body" ]
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
	const auto& upsert = embedding.value().upsertEmbedder.value();
	EXPECT_EQ(upsert.fieldsFormat, FloatVectorIndexOpts::EmbedderOpts::FieldsFormat::Join);
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefFieldsFormatWithoutOpenAI) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777",
				"fields_format": "stringify",
				"fields": [ "title" ]
			}
		}
	}
}
)json"sv);
	ASSERT_FALSE(indexDef);
	ASSERT_STREQ(indexDef.error().what(), "Configuration 'embedding:upsert_embedder' field 'fields_format' must be set inside 'protocol'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefOpenAIAllowsScalarFloatVector) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:8080/v1/embeddings",
				"protocol": {
					"type": "openai",
					"model": "local"
				},
				"fields": [ "title" ]
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	EXPECT_FALSE(indexDef->Opts().IsArray());
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefDefaultProtocolIsRx) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"query_embedder": {
				"URL": "http://127.0.0.1:7777"
			}
		}
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
	auto embedding = indexDef->Opts().FloatVector().Embedding();
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
	const auto& query = embedding.value().queryEmbedder.value();
	EXPECT_EQ(query.protocol, FloatVectorIndexOpts::EmbedderOpts::Protocol::Rx);
	EXPECT_TRUE(query.model.empty());
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefOpenAIWithoutModel) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"query_embedder": {
				"URL": "http://127.0.0.1:8080/v1/embeddings",
				"protocol": {
					"type": "openai"
				}
			}
		}
	}
}
)json"sv);
	ASSERT_FALSE(indexDef);
	ASSERT_STREQ(indexDef.error().what(), "Configuration 'embedding:query_embedder' with protocol 'openai' must contain field 'model'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefUnexpectedProtocol) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"query_embedder": {
				"URL": "http://127.0.0.1:8080/v1/embeddings",
				"protocol": {
					"type": "grpc"
				}
			}
		}
	}
}
)json"sv);
	ASSERT_FALSE(indexDef);
	ASSERT_STREQ(
		indexDef.error().what(),
		"Configuration 'embedding:query_embedder:protocol' unexpected field value 'type'. Set 'grpc', but expected 'rx' or 'openai'");

	const auto missingType = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"query_embedder": {
				"URL": "http://127.0.0.1:8080/v1/embeddings",
				"protocol": {}
			}
		}
	}
}
)json"sv);
	ASSERT_FALSE(missingType);
	ASSERT_STREQ(missingType.error().what(), "Configuration 'embedding:query_embedder:protocol' must contain field 'type'");
}
CATCH_AND_ASSERT

TEST(EmbeddingConfig, RejectsProtocolSpecificOptionsInWrongContext) {
	auto validate = [](FloatVectorIndexOpts::EmbedderOpts embedder, bool upsert) {
		FloatVectorIndexOpts opts{IndexHnsw};
		std::ignore = opts.SetDimension(4);
		FloatVectorIndexOpts::EmbeddingOpts embedding;
		if (upsert) {
			embedding.upsertEmbedder = std::move(embedder);
		} else {
			embedding.queryEmbedder = std::move(embedder);
		}
		std::ignore = opts.SetEmbedding(std::move(embedding));
		opts.Validate(IndexHnsw);
	};

	FloatVectorIndexOpts::EmbedderOpts rxWithModel;
	rxWithModel.endpointUrl = "http://127.0.0.1:8080";
	rxWithModel.model = "unused";
	EXPECT_THROW(validate(std::move(rxWithModel), false), reindexer::Error);

	FloatVectorIndexOpts::EmbedderOpts queryWithFieldsFormat;
	queryWithFieldsFormat.endpointUrl = "http://127.0.0.1:8080/v1/embeddings";
	queryWithFieldsFormat.protocol = FloatVectorIndexOpts::EmbedderOpts::Protocol::OpenAI;
	queryWithFieldsFormat.model = "local";
	queryWithFieldsFormat.fieldsFormat = FloatVectorIndexOpts::EmbedderOpts::FieldsFormat::Join;
	EXPECT_THROW(validate(std::move(queryWithFieldsFormat), false), reindexer::Error);

	FloatVectorIndexOpts::EmbedderOpts httpsOpenAI;
	httpsOpenAI.endpointUrl = "https://api.openai.com/v1/embeddings";
	httpsOpenAI.protocol = FloatVectorIndexOpts::EmbedderOpts::Protocol::OpenAI;
	httpsOpenAI.model = "text-embedding-3-small";
	EXPECT_THROW(validate(std::move(httpsOpenAI), false), reindexer::Error);

	FloatVectorIndexOpts::EmbedderOpts ftpOpenAI;
	ftpOpenAI.endpointUrl = "ftp://localhost/v1/embeddings";
	ftpOpenAI.protocol = FloatVectorIndexOpts::EmbedderOpts::Protocol::OpenAI;
	ftpOpenAI.model = "text-embedding-3-small";
	EXPECT_THROW(validate(std::move(ftpOpenAI), false), reindexer::Error);

	FloatVectorIndexOpts::EmbedderOpts unknownSchemeOpenAI;
	unknownSchemeOpenAI.endpointUrl = "custom://localhost/v1/embeddings";
	unknownSchemeOpenAI.protocol = FloatVectorIndexOpts::EmbedderOpts::Protocol::OpenAI;
	unknownSchemeOpenAI.model = "text-embedding-3-small";
	EXPECT_THROW(validate(std::move(unknownSchemeOpenAI), false), reindexer::Error);

	FloatVectorIndexOpts::EmbedderOpts missingHostOpenAI;
	missingHostOpenAI.endpointUrl = "http://";
	missingHostOpenAI.protocol = FloatVectorIndexOpts::EmbedderOpts::Protocol::OpenAI;
	missingHostOpenAI.model = "text-embedding-3-small";
	EXPECT_THROW(validate(std::move(missingHostOpenAI), false), reindexer::Error);

	FloatVectorIndexOpts::EmbedderOpts ipv6OpenAI;
	ipv6OpenAI.endpointUrl = "http://[::1]/v1/embeddings";
	ipv6OpenAI.protocol = FloatVectorIndexOpts::EmbedderOpts::Protocol::OpenAI;
	ipv6OpenAI.model = "text-embedding-3-small";
	EXPECT_THROW(validate(std::move(ipv6OpenAI), false), reindexer::Error);
}

TEST(EmbeddingProtocol, ResolvesOpenAIEndpoint) {
	const auto& protocol = reindexer::embedding::GetEmbedProtocol(reindexer::EmbedderConfig::Protocol::OpenAI);

	auto endpoint = protocol.ResolveEndpoint("http://localhost", "", "");
	EXPECT_EQ(endpoint.baseUrl, "http://localhost");
	EXPECT_EQ(endpoint.path, "/v1/embeddings");

	endpoint = protocol.ResolveEndpoint("http://localhost/custom/embeddings?api-version=1", "", "");
	EXPECT_EQ(endpoint.baseUrl, "http://localhost");
	EXPECT_EQ(endpoint.path, "/custom/embeddings?api-version=1");

	EXPECT_THROW(
		{ [[maybe_unused]] const auto invalidEndpoint = protocol.ResolveEndpoint("http://[::1]/v1/embeddings", "", ""); },
		reindexer::Error);

	const auto& rxProtocol = reindexer::embedding::GetEmbedProtocol(reindexer::EmbedderConfig::Protocol::Rx);
	endpoint = rxProtocol.ResolveEndpoint("http://localhost:8080", "embedder", "json");
	EXPECT_EQ(endpoint.baseUrl, "http://localhost:8080");
	EXPECT_EQ(endpoint.path, "/api/v1/embedder/embedder/produce?format=json");
}

TEST(EmbeddingAdapter, OpenAIRequestAndResponse) {
	using reindexer::EmbedderConfig;
	using reindexer::Variant;
	using reindexer::VariantArray;
	using reindexer::embedding::Adapter;
	using reindexer::embedding::ValueT;

	Adapter queryAdapter("hello world", EmbedderConfig::Protocol::OpenAI, "local");
	auto queryChunk = queryAdapter.Content();
	const std::string queryJson{reinterpret_cast<char*>(queryChunk.data()), queryChunk.size()};
	EXPECT_EQ(queryJson, R"({"model":"local","input":"hello world"})");
	EXPECT_EQ(queryAdapter.CacheKey(), "openai-v1:" + queryJson);

	Adapter emptyQueryAdapter("", EmbedderConfig::Protocol::OpenAI, "local");
	auto emptyQueryChunk = emptyQueryAdapter.Content();
	const std::string emptyQueryJson{reinterpret_cast<char*>(emptyQueryChunk.data()), emptyQueryChunk.size()};
	EXPECT_EQ(emptyQueryJson, R"({"model":"local","input":""})");
	EXPECT_EQ(emptyQueryAdapter.CacheKey(), "openai-v1:" + emptyQueryJson);

	std::vector<std::pair<std::string, VariantArray>> doc{
		{"title", VariantArray{Variant{"t1"}}},
		{"tags", VariantArray{Variant{"a"}, Variant{"b"}}},
	};
	std::vector<std::vector<std::pair<std::string, VariantArray>>> sources{doc};
	Adapter upsertJoin(sources, EmbedderConfig::Protocol::OpenAI, EmbedderConfig::FieldsFormat::Join, "local");
	auto upsertJoinChunk = upsertJoin.Content();
	const std::string upsertJoinJson{reinterpret_cast<char*>(upsertJoinChunk.data()), upsertJoinChunk.size()};
	EXPECT_EQ(upsertJoinJson, R"({"model":"local","input":"t1\na b"})");
	EXPECT_EQ(upsertJoin.CacheKey(), "openai-v1:" + upsertJoinJson);

	std::vector<std::pair<std::string, VariantArray>> emptyDoc{{"title", VariantArray{Variant{""}}}};
	std::vector<std::vector<std::pair<std::string, VariantArray>>> emptySources{emptyDoc};
	Adapter emptyJoin(emptySources, EmbedderConfig::Protocol::OpenAI, EmbedderConfig::FieldsFormat::Join, "local");
	auto emptyJoinChunk = emptyJoin.Content();
	const std::string emptyJoinJson{reinterpret_cast<char*>(emptyJoinChunk.data()), emptyJoinChunk.size()};
	EXPECT_EQ(emptyJoinJson, R"({"model":"local","input":""})");

	Adapter upsertJson(sources, EmbedderConfig::Protocol::OpenAI, EmbedderConfig::FieldsFormat::Stringify, "local");
	auto upsertJsonChunk = upsertJson.Content();
	const std::string upsertJsonStr{reinterpret_cast<char*>(upsertJsonChunk.data()), upsertJsonChunk.size()};
	EXPECT_EQ(upsertJsonStr, R"({"model":"local","input":"{\"title\":\"t1\",\"tags\":[\"a\",\"b\"]}"})");
	EXPECT_EQ(upsertJson.CacheKey(), "openai-v1:" + upsertJsonStr);
	EXPECT_NE(upsertJoin.CacheKey(), upsertJson.CacheKey());

	Adapter otherModel(sources, EmbedderConfig::Protocol::OpenAI, EmbedderConfig::FieldsFormat::Stringify, "other");
	EXPECT_NE(upsertJson.CacheKey(), otherModel.CacheKey());

	auto batchSources = sources;
	batchSources.push_back({{"title", VariantArray{Variant{"t2"}}}, {"tags", VariantArray{Variant{"c"}}}});
	EXPECT_THROW(
		{ [[maybe_unused]] Adapter a(batchSources, EmbedderConfig::Protocol::OpenAI, EmbedderConfig::FieldsFormat::Stringify, "local"); },
		reindexer::Error);

	Adapter rxAdapter(sources, EmbedderConfig::Protocol::Rx, EmbedderConfig::FieldsFormat::Stringify, "");
	auto rxChunk = rxAdapter.Content();
	const std::string rxJson{reinterpret_cast<char*>(rxChunk.data()), rxChunk.size()};
	EXPECT_EQ(rxJson, R"({"data":[{"title":"t1","tags":["a","b"]}]})");
	EXPECT_EQ(rxAdapter.CacheKey(), rxAdapter.View());

	std::vector<std::pair<std::string, VariantArray>> collisionDoc{{"model", VariantArray{Variant{"local"}}},
																   {"input", VariantArray{Variant{"hello world"}}}};
	std::vector<std::vector<std::pair<std::string, VariantArray>>> collisionSources{collisionDoc};
	Adapter collisionRx(collisionSources, EmbedderConfig::Protocol::Rx, EmbedderConfig::FieldsFormat::Stringify, "");
	EXPECT_NE(queryAdapter.CacheKey(), collisionRx.CacheKey());

	ValueT products;
	const auto err = Adapter::VectorsFromJSON(R"({
		"object":"list",
		"data":[
			{"object":"embedding","index":0,"embedding":[0.0,0.1]}
		]
	})",
											  EmbedderConfig::Protocol::OpenAI, products);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(products.size(), 1);
	ASSERT_EQ(products[0].Dimension().Value(), 2);
	EXPECT_FLOAT_EQ(products[0].RawData()[0], 0.0f);
	EXPECT_FLOAT_EQ(products[0].RawData()[1], 0.1f);

	products.clear();
	const auto rxErr = Adapter::VectorsFromJSON(R"({"products":[[{"embedding":[0.2,0.3]}]]})", EmbedderConfig::Protocol::Rx, products);
	ASSERT_TRUE(rxErr.ok()) << rxErr.what();
	ASSERT_EQ(products.size(), 1);
	EXPECT_FLOAT_EQ(products[0].RawData()[0], 0.2f);
	EXPECT_FLOAT_EQ(products[0].RawData()[1], 0.3f);
}

TEST(EmbeddingAdapter, RejectsMalformedOpenAIResponse) {
	using reindexer::EmbedderConfig;
	using reindexer::embedding::Adapter;
	using reindexer::embedding::ValueT;

	ValueT products;
	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({})", EmbedderConfig::Protocol::OpenAI, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"data":[{"index":0,"embedding":[0.1]},{"index":0,"embedding":[0.2]}]})",
										  EmbedderConfig::Protocol::OpenAI, products)
					 .ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"data":[{"index":1,"embedding":[0.1]}]})", EmbedderConfig::Protocol::OpenAI, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"data":[{"embedding":[0.1]}]})", EmbedderConfig::Protocol::OpenAI, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"data":[{"index":0}]})", EmbedderConfig::Protocol::OpenAI, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(
		Adapter::VectorsFromJSON(R"({"data":[{"index":0,"embedding":"not_an_array"}]})", EmbedderConfig::Protocol::OpenAI, products).ok());
	EXPECT_TRUE(products.empty());
}

TEST(EmbeddingAdapter, ParsesEmptyEmbeddingAsEmptyVector) {
	using reindexer::EmbedderConfig;
	using reindexer::embedding::Adapter;
	using reindexer::embedding::ValueT;

	ValueT products;
	const auto rxErr = Adapter::VectorsFromJSON(R"({"products":[[{"embedding":[]}]]})", EmbedderConfig::Protocol::Rx, products);
	ASSERT_TRUE(rxErr.ok()) << rxErr.what();
	ASSERT_EQ(products.size(), 1);
	EXPECT_TRUE(products[0].IsEmpty());

	products.clear();
	const auto openaiErr = Adapter::VectorsFromJSON(R"({"data":[{"index":0,"embedding":[]}]})", EmbedderConfig::Protocol::OpenAI, products);
	ASSERT_TRUE(openaiErr.ok()) << openaiErr.what();
	ASSERT_EQ(products.size(), 1);
	EXPECT_TRUE(products[0].IsEmpty());

	products.clear();
	const auto rxNullErr = Adapter::VectorsFromJSON(R"({"products":[[{"embedding":null}]]})", EmbedderConfig::Protocol::Rx, products);
	ASSERT_TRUE(rxNullErr.ok()) << rxNullErr.what();
	ASSERT_EQ(products.size(), 1);
	EXPECT_TRUE(products[0].IsEmpty());

	products.clear();
	const auto openaiNullErr =
		Adapter::VectorsFromJSON(R"({"data":[{"index":0,"embedding":null}]})", EmbedderConfig::Protocol::OpenAI, products);
	ASSERT_TRUE(openaiNullErr.ok()) << openaiNullErr.what();
	ASSERT_EQ(products.size(), 1);
	EXPECT_TRUE(products[0].IsEmpty());
}

TEST(EmbeddingAdapter, RejectsMalformedRxResponse) {
	using reindexer::EmbedderConfig;
	using reindexer::embedding::Adapter;
	using reindexer::embedding::ValueT;

	ValueT products;
	EXPECT_FALSE(Adapter::VectorsFromJSON("null", EmbedderConfig::Protocol::Rx, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({})", EmbedderConfig::Protocol::Rx, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"wrong_key":"wrong_value"})", EmbedderConfig::Protocol::Rx, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"products":"not_a_list"})", EmbedderConfig::Protocol::Rx, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"products":[{"embedding":[0.1]}]})", EmbedderConfig::Protocol::Rx, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"products":[[{"embedding":"not_an_array"}]]})", EmbedderConfig::Protocol::Rx, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(Adapter::VectorsFromJSON(R"({"products":[[{}]]})", EmbedderConfig::Protocol::Rx, products).ok());
	EXPECT_TRUE(products.empty());

	EXPECT_FALSE(
		Adapter::VectorsFromJSON(R"({"products":[[{"embedding":["not_a_number"]}]]})", EmbedderConfig::Protocol::Rx, products).ok());
	EXPECT_TRUE(products.empty());
}

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderUrl) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ]
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(), "Configuration 'embedding:upsert_embedder' must contain field 'URL' and 'fields'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderUrlNot) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "Y.GAGARIN"
				"fields": [ "idx1" ]
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(),
				 "Configuration 'embedding:upsert_embedder' contain field 'URL' with unexpected value: 'Y.GAGARIN'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderFields) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": []
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(), "Configuration 'embedding:upsert_embedder' must contain field 'URL' and 'fields'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderFieldsEmpty) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": ["", "1"]
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(), "Configuration 'embedding:upsert_embedder' does not support empty field names");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderFieldsDuplicate) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "Fld1", "Fld2", "Fld1" ]
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(),
				 "Configuration 'embedding:upsert_embedder' does not support duplicate field names. Duplicate 'Fld1'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderStrategy) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ],
				"embedding_strategy": "never"
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(),
				 "Configuration 'embedding:upsert_embedder' unexpected field value 'embedding_strategy'. Set 'never', but expected "
				 "'always', 'empty_only' or 'strict'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderPoolConnections) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ]
				"pool": {
					"connections": 0,
					"connect_timeout_ms": 100,
					"read_timeout_ms": 1000,
					"write_timeout_ms": 1000
				}
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(), "Configuration 'embedding:upsert_embedder:pool:connections' should not be less than 1");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderPoolConnectTM) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ]
				"pool": {
					"connections": 10,
					"connect_timeout_ms": 10,
					"read_timeout_ms": 1000,
					"write_timeout_ms": 1000
				}
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(),
				 "Configuration 'embedding:upsert_embedder:pool:connect_timeout_ms' should not be less than 100 ms, in config '10'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderPoolReadTM) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ]
				"pool": {
					"connections": 10,
					"connect_timeout_ms": 100,
					"read_timeout_ms": 499,
					"write_timeout_ms": 1000
				}
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(),
				 "Configuration 'embedding:upsert_embedder:pool:read_timeout_ms' should not be less than 500 ms, in config '499'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeParseDslIndexDefWithEmbedderPoolWriteTM) try {
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
		"embedding": {
			"upsert_embedder": {
				"URL": "http://127.0.0.1:7777/embedder",
				"cache_tag": "UpsertEmbedder",
				"fields": [ "idx1", "idx2" ]
				"pool": {
					"connections": 10,
					"connect_timeout_ms": 100,
					"read_timeout_ms": 500,
					"write_timeout_ms": 127
				}
			}
		}
	}
}
)json"sv);
	ASSERT_STREQ(indexDef.error().what(),
				 "Configuration 'embedding:upsert_embedder:pool:write_timeout_ms' should not be less than 500 ms, in config '127'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeWrongField) try {
	static const std::string kNsName{"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameNone{"not_yet"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {std::string(kFieldNameNone)};  // NOTE: maybe-uninitialized

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.AddIndex(kNsName,
				{kFieldNameIvf,
				 {kFieldNameIvf},
				 "float_vector",
				 IndexOpts{}.SetFloatVector(
					 IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding))});
	Item item(rt.NewItem(kNsName));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = rt.reindexer->Upsert(kNsName, item);
	ASSERT_EQ(err.what(), "Cannot automatically embed value in index field named '" + kFieldNameIvf + "'. The auxiliary field '" +
							  kFieldNameNone +
							  "' was not found or is a composite or sparse field. Embedding is supported only for scalar index fields");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeSparseField) try {
	static const std::string kNsName{"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameSparse{"sparse"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {std::string(kFieldNameSparse)};	// NOTE: maybe-uninitialized

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
										IndexDeclaration{kFieldNameSparse, "hash", "string", IndexOpts{}.Sparse(), 0}});
	rt.AddIndex(kNsName,
				{kFieldNameIvf,
				 {kFieldNameIvf},
				 "float_vector",
				 IndexOpts{}.SetFloatVector(
					 IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding))});
	Item item(rt.NewItem(kNsName));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = rt.reindexer->Upsert(kNsName, item);
	ASSERT_EQ(err.what(), "Cannot automatically embed value in index field named '" + kFieldNameIvf + "'. The auxiliary field '" +
							  kFieldNameSparse +
							  "' was not found or is a composite or sparse field. Embedding is supported only for scalar index fields");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeCompositeField) try {
	static const std::string kNsName{"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameFirst{"first"};
	static const std::string kFieldNameComposite{"composite"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {kFieldNameFirst, kFieldNameComposite};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
										IndexDeclaration{kFieldNameFirst, "hash", "int", IndexOpts(), 0},
										IndexDeclaration{kFieldNameComposite, "text", "composite", IndexOpts(), 0}});
	rt.AddIndex(kNsName,
				{kFieldNameIvf,
				 {kFieldNameIvf},
				 "float_vector",
				 IndexOpts{}.SetFloatVector(
					 IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding))});
	Item item(rt.NewItem(kNsName));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = rt.reindexer->Upsert(kNsName, item);
	ASSERT_EQ(err.what(), "Cannot automatically embed value in index field named '" + kFieldNameIvf + "'. The auxiliary field '" +
							  kFieldNameComposite +
							  "' was not found or is a composite or sparse field. Embedding is supported only for scalar index fields");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeDropIndex) try {
	static const std::string kNsName{"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameFirst{"first"};
	static const std::string kFieldNameSecond{"second"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {kFieldNameFirst, kFieldNameSecond};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
				  IndexDeclaration{kFieldNameFirst, "hash", "string", IndexOpts(), 0},
				  IndexDeclaration{kFieldNameSecond, "hash", "string", IndexOpts(), 0},
				  IndexDeclaration{
					  kFieldNameIvf, "ivf", "float_vector",
					  IndexOpts{}.SetFloatVector(
						  IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding)),
					  0}});
	auto err = rt.reindexer->DropIndex(kNsName, reindexer::IndexDef{kFieldNameSecond});
	ASSERT_EQ(err.what(),
			  "Cannot remove index '" + kFieldNameSecond + "': it's a part of a auto embedding logic in index '" + kFieldNameIvf + "'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, SqlQueryWhereKNN) try {
	using reindexer::Query;
	using reindexer::KnnSearchParams;
	static const std::string k1st("Pocomaxa");
	static const std::string k2nd("Hi, bro!");
	static const std::string k3rd("man`s word: blood on the asphalt");
	struct {
		Query query;
		std::string sql;
	} testData[]{
		{Query("ns"sv).WhereKNN("hnsw"sv, k1st, reindexer::HnswSearchParams{}.K(4'291).Radius(1.043).Ef(100'000)).SelectAllFields(),
		 "SELECT *, vectors() FROM ns WHERE KNN(hnsw, \'" + k1st + "\', k=4291, radius=1.043, ef=100000)"},
		{Query("ns"sv).WhereKNN("bf"sv, k2nd, reindexer::BruteForceSearchParams{}.K(8'184)).Select("vectors()"),
		 "SELECT vectors() FROM ns WHERE KNN(bf, \'" + k2nd + "\', k=8184)"},
		{Query("ns"sv).WhereKNN("ivf"sv, k3rd, reindexer::IvfSearchParams{}.K(823).Radius(1.32).NProbe(5)).Select("hnsw"sv, "vectors()"),
		 "SELECT hnsw, vectors() FROM ns WHERE KNN(ivf, \'" + k3rd + "\', k=823, radius=1.32, nprobe=5)"}};
	for (const auto& [query, expectedSql] : testData) {
		const auto generatedSql = query.GetSQL();
		EXPECT_EQ(generatedSql, expectedSql);
		const auto parsedQuery = Query::FromSQL(expectedSql);
		EXPECT_EQ(parsedQuery, query) << "original: " << expectedSql << "\nparsed: " << parsedQuery.GetSQL();
	}
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeWrongFieldConfigUpdate) try {
	static const std::string kNsName{"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameNone{"not_yet"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {kFieldNameId};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
				  IndexDeclaration{
					  kFieldNameIvf, "ivf", "float_vector",
					  IndexOpts{}.SetFloatVector(
						  IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding)),
					  0}});
	embedder.fields = {kFieldNameNone};
	embedding.upsertEmbedder = embedder;
	auto updateIdx = reindexer::IndexDef(
		kFieldNameIvf, "ivf", "float_vector",
		IndexOpts{}.SetFloatVector(IndexIvf,
								   FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding)));
	auto err = rt.reindexer->UpdateIndex(kNsName, updateIdx);
	ASSERT_EQ(err.what(), "Cannot update index field named '" + kFieldNameIvf + "' in namespace '" + kNsName + "'. Auxiliary field '" +
							  kFieldNameNone + "' not found");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeSparseFieldConfigUpdate) try {
	static const std::string kNsName{"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameSparse{"sparse"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {kFieldNameId};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
				  IndexDeclaration{kFieldNameSparse, "hash", "string", IndexOpts{}.Sparse(), 0},
				  IndexDeclaration{
					  kFieldNameIvf, "ivf", "float_vector",
					  IndexOpts{}.SetFloatVector(
						  IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding)),
					  0}});
	embedder.fields = {kFieldNameSparse};
	embedding.upsertEmbedder = embedder;
	auto updateIdx = reindexer::IndexDef(
		kFieldNameIvf, "ivf", "float_vector",
		IndexOpts{}.SetFloatVector(IndexIvf,
								   FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding)));
	auto err = rt.reindexer->UpdateIndex(kNsName, updateIdx);
	ASSERT_EQ(err.what(), "Cannot update index field named '" + kFieldNameIvf + "' in namespace '" + kNsName +
							  "'. Support for embedding only for scalar index fields. Using field '" + kFieldNameSparse +
							  "' is sparse, so embedding is not supported");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, NegativeCompositeFieldConfigUpdate) try {
	static const std::string kNsName = {"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameComposite{"composite"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {kFieldNameId};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
				  IndexDeclaration{kFieldNameComposite, "text", "composite", IndexOpts(), 0},
				  IndexDeclaration{
					  kFieldNameIvf, "ivf", "float_vector",
					  IndexOpts{}.SetFloatVector(
						  IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding)),
					  0}});
	embedder.fields = {kFieldNameId, kFieldNameComposite};
	embedding.upsertEmbedder = embedder;
	auto updateIdx = reindexer::IndexDef(
		kFieldNameIvf, "ivf", "float_vector",
		IndexOpts{}.SetFloatVector(IndexIvf,
								   FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding)));
	auto err = rt.reindexer->UpdateIndex(kNsName, updateIdx);
	ASSERT_EQ(err.what(), "Cannot update index field named '" + kFieldNameIvf + "' in namespace '" + kNsName +
							  "'. Support for embedding only for scalar index fields. Using composite field '" + kFieldNameComposite +
							  "' for embedding is invalid");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ConfigUpdateOnlyEmbedders) try {
	static const std::string kNsName = {"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameFirst{"first"};
	static const std::string kFieldNameSecond{"second"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8001";
	embedder.fields = {kFieldNameFirst};
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly;
	embedder.cacheTag = "upsertEmbedderOne";
	embedder.pool.connections = 11;
	embedder.pool.connect_timeout_ms = 1111;
	embedder.pool.write_timeout_ms = 1112;
	embedder.pool.read_timeout_ms = 1113;

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	embedder.endpointUrl = "http://127.0.0.1:8002";
	embedder.fields = {kFieldNameSecond};
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::Always;
	embedder.cacheTag = "queryEmbedderOne";
	embedder.pool.connections = 12;
	embedder.pool.connect_timeout_ms = 1121;
	embedder.pool.write_timeout_ms = 1122;
	embedder.pool.read_timeout_ms = 1123;
	embedding.queryEmbedder = embedder;

	std::vector<reindexer::IndexDef> idxs{
		reindexer::IndexDef{kFieldNameId, "hash", "int", IndexOpts().PK()},
		reindexer::IndexDef{kFieldNameFirst, "hash", "string", IndexOpts()},
		reindexer::IndexDef{
			kFieldNameIvf, "ivf", "float_vector",
			IndexOpts{}.SetFloatVector(
				IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding))},
		reindexer::IndexDef{kFieldNameSecond, "hash", "string", IndexOpts()}};
	rt.OpenNamespace(kNsName);
	for (const auto& idx : idxs) {
		rt.AddIndex(kNsName, idx);
	}

	embedder.endpointUrl = "http://0.0.0.0:8001";
	embedder.fields = {kFieldNameId, kFieldNameSecond};
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::Strict;
	embedder.cacheTag = "queryEmbedderSecond";
	embedder.pool.connections = 21;
	embedder.pool.connect_timeout_ms = 1211;
	embedder.pool.write_timeout_ms = 1212;
	embedder.pool.read_timeout_ms = 1213;
	embedding.upsertEmbedder = embedder;

	embedder.endpointUrl = "http://0.0.0.0:8002";
	embedder.fields = {kFieldNameId};
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly;
	embedder.cacheTag = "queryEmbedderSecond";
	embedder.pool.connections = 22;
	embedder.pool.connect_timeout_ms = 1221;
	embedder.pool.write_timeout_ms = 1222;
	embedder.pool.read_timeout_ms = 1223;
	embedding.queryEmbedder = embedder;

	auto updateIdx = reindexer::IndexDef(
		kFieldNameIvf, "ivf", "float_vector",
		IndexOpts{}.SetFloatVector(IndexIvf,
								   FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements).SetEmbedding(embedding)));
	rt.UpdateIndex(kNsName, updateIdx);

	auto nsList = rt.EnumNamespaces(reindexer::EnumNamespacesOpts().HideSystem());
	for (const auto& ns : nsList) {
		uint32_t i = 0;
		for (const auto& index : ns.indexes) {
			const auto& idx = idxs[i++];
			ASSERT_EQ(idx.Name(), index.Name());
			if (index.Name() == kFieldNameIvf) {
				ASSERT_TRUE(updateIdx.Compare(index).Equal());
			} else {
				ASSERT_TRUE(index.Compare(idx).Equal());
			}
		}
	}
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ConfigUpdate) try {
	static const std::string kNsName = {"ivf_ns"};
	static const std::string kFieldNameIvf{"ivf"};
	static const std::string kFieldNameFirst{"first"};
	static constexpr size_t kDimension = 32;
	static constexpr size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8001";
	embedder.fields = {kFieldNameId};
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly;
	embedder.cacheTag = "upsertEmbedder";

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	embedder.endpointUrl = "http://127.0.0.1:8002";
	embedder.fields = {kFieldNameId};
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::Always;
	embedder.cacheTag = "queryEmbedderOne";
	embedder.pool.connections = 1;
	embedding.queryEmbedder = embedder;

	std::vector<reindexer::IndexDef> idxs{
		reindexer::IndexDef{kFieldNameId, "hash", "int", IndexOpts().PK()},
		reindexer::IndexDef{kFieldNameIvf, "ivf", "float_vector",
							IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																	 .SetDimension(kDimension)
																	 .SetNCentroids(kMaxElements)
																	 .SetEmbedding(embedding)
																	 .SetMetric(reindexer::VectorMetric::InnerProduct))},
		reindexer::IndexDef{kFieldNameFirst, "hash", "string", IndexOpts()}};
	rt.OpenNamespace(kNsName);
	for (const auto& idx : idxs) {
		rt.AddIndex(kNsName, idx);
	}

	auto updateIdx = reindexer::IndexDef(kFieldNameIvf, "ivf", "float_vector",
										 IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																				  .SetDimension(kDimension)
																				  .SetNCentroids(kMaxElements)
																				  .SetEmbedding(embedding)
																				  .SetMetric(reindexer::VectorMetric::L2)));
	rt.UpdateIndex(kNsName, updateIdx);

	auto nsList = rt.EnumNamespaces(reindexer::EnumNamespacesOpts().HideSystem());
	for (const auto& ns : nsList) {
		for (const auto& index : ns.indexes) {
			auto it = std::ranges::find_if(idxs, [&index](const reindexer::IndexDef& dec) { return index.Name() == dec.Name(); });
			ASSERT_TRUE(it != idxs.end());
			if (index.Name() == kFieldNameIvf) {
				ASSERT_TRUE(updateIdx.Compare(index).Equal());
				ASSERT_TRUE(ns.indexes.back().Name() == it->Name());
			} else {
				ASSERT_TRUE(index.Compare(*it).Equal());
			}
		}
	}
}
CATCH_AND_ASSERT

static void upsertItems(ReindexerTestApi<reindexer::Reindexer>& rt, std::string_view nsName, std::string_view fieldName, size_t dimension,
						int startId, int endId) {
	assert(startId < endId);
	std::vector<float> buf(dimension);
	for (int id = startId; id < endId; ++id) {
		for (float& v : buf) {
			v = reindexer_tests_tools::randBin<float>(-10, 10);
		}
		auto item = rt.NewItem(nsName);
		item[kFieldNameId] = id;
		item[fieldName] = reindexer::ConstFloatVectorView{buf};
		rt.Upsert(nsName, item);
	}
}

TEST_F(EmbeddingTest, NegativeActionCreateEmbedding) try {
	constexpr static auto kNsName = "ivf_ns"sv;
	const static std::string kFieldNameIvf{"ivf"};
	const static std::string kFieldNameFirst{"first"};
	const static std::string kFieldNameSecond{"second"};
	constexpr static size_t kDimension = 32;
	constexpr static size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {kFieldNameFirst, kFieldNameSecond};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
										IndexDeclaration{kFieldNameFirst, "hash", "string", IndexOpts(), 0},
										IndexDeclaration{kFieldNameSecond, "hash", "string", IndexOpts(), 0},
										IndexDeclaration{kFieldNameIvf, "ivf", "float_vector",
														 IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																								  .SetDimension(kDimension)
																								  .SetNCentroids(kMaxElements / 50)
																								  .SetMetric(reindexer::VectorMetric::L2)
																								  .SetEmbedding(embedding)),
														 0}});
	upsertItems(rt, kNsName, kFieldNameIvf, kDimension, 0, 100);

	Error err;
	auto item = rt.NewItem(reindexer::kConfigNamespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	err = item.FromJSON(
		R"json({"type":"action","action":{"command":"create_embeddings", "namespace":"WeDontHaveThisNSNow", "batch_size":100}})json");
	ASSERT_TRUE(err.ok()) << err.what();
	rt.Upsert(reindexer::kConfigNamespace, item);

	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::Strict;
	embedding.upsertEmbedder = embedder;
	auto updateIdx = reindexer::IndexDef(kFieldNameIvf, "ivf", "float_vector",
										 IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																				  .SetDimension(kDimension)
																				  .SetNCentroids(kMaxElements / 50)
																				  .SetMetric(reindexer::VectorMetric::L2)
																				  .SetEmbedding(embedding)));
	rt.UpdateIndex(kNsName, updateIdx);

	err = item.FromJSON(R"json({"type":"action","action":{"command":"create_embeddings", "namespace":"*", "batch_size":100}})json");
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Upsert(reindexer::kConfigNamespace, item);
	ASSERT_EQ(err.what(),
			  "Vector field '" + kFieldNameIvf + "' must not contain a non-empty data if strict strategy for auto-embedding is configured");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, SkipEmbeddingStrategy) {
	constexpr static auto kNsName = "ns_skip_embedding"sv;
	const static std::string kFieldNameIvf{"ivf_index"};
	const static std::string kFieldNameFirst{"first"};
	const static std::string kFieldNameSecond{"second"};
	constexpr static size_t kDimension = 3;
	constexpr static size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.strategy = FloatVectorIndexOpts::EmbedderOpts::Strategy::Always;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {kFieldNameFirst, kFieldNameSecond};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	IndexDeclaration vecIndex{kFieldNameIvf, "ivf", "float_vector",
							  IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																	   .SetDimension(kDimension)
																	   .SetNCentroids(kMaxElements / 50)
																	   .SetMetric(reindexer::VectorMetric::L2)
																	   .SetEmbedding(embedding)),
							  0};

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
										IndexDeclaration{kFieldNameFirst, "hash", "string", IndexOpts(), 0},
										IndexDeclaration{kFieldNameSecond, "hash", "string", IndexOpts(), 0}, vecIndex});
	auto check = [&]() {
		std::vector<float> buf{1.0, 2.0, 3.0};
		auto item = rt.NewItem(kNsName);
		item[kFieldNameId] = 0;
		item[kFieldNameIvf] = reindexer::ConstFloatVectorView{buf};
		std::vector<std::string> precepts;
		precepts.emplace_back(kFieldNameIvf + "=skip_embedding()");
		item.SetPrecepts(precepts);
		rt.Upsert(kNsName, item);
		Query q{Query::FromSQL("Select *,vectors() from ns_skip_embedding")};
		{
			auto qr = rt.Select(q);
			EXPECT_EQ(qr.Count(), 1);
			EXPECT_EQ((*qr.begin()).GetJSON(), R"#({"id":0,"first":"","second":"","ivf_index":[1.0,2.0,3.0]})#");
		}
	};
	check();

	auto changeIndexAndCheck = [&](FloatVectorIndexOpts::EmbedderOpts::Strategy s) {
		rt.TruncateNamespace(kNsName);
		embedding.upsertEmbedder.value().strategy = s;
		IndexOpts vecIndexE = IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																	   .SetDimension(kDimension)
																	   .SetNCentroids(kMaxElements / 50)
																	   .SetMetric(reindexer::VectorMetric::L2)
																	   .SetEmbedding(embedding));

		auto updateIdx = reindexer::IndexDef{kFieldNameIvf, "ivf", "float_vector", vecIndexE};
		rt.UpdateIndex(kNsName, updateIdx);
		check();
	};
	changeIndexAndCheck(FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly);
	changeIndexAndCheck(FloatVectorIndexOpts::EmbedderOpts::Strategy::Strict);
}

}  // namespace reindexer_tests
