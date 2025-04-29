#include "gtests/tests/fixtures/embedding_test.h"
#include <gmock/gmock.h>
#include "gtests/tools.h"
#include "tools/errors.h"

using namespace std::string_view_literals;
static constexpr std::string_view kFieldNameId = "id"sv;

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbedding) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithQueryEmbedding) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithQueryEmbeddingOnly) try {
	using namespace std::string_literals;
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
	// NOLINTBEGIN(bugprone-unchecked-optional-access)
	const auto& embedOpts = embedding.value();
	// NOLINTEND(bugprone-unchecked-optional-access)
	ASSERT_TRUE(!embedOpts.upsertEmbedder.has_value());
	ASSERT_TRUE(embedOpts.queryEmbedder.has_value());
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegative) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingPool) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingPoolFull) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativeUrl) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativeFields) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativeFieldsEmpty) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativeFieldsDuplicate) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativeStrategy) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativePoolConnections) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativePoolConnectTM) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativePoolReadTM) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, ParseDslIndexDefWithEmbeddingNegativePoolWriteTM) try {
	using namespace std::string_literals;
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

TEST_F(EmbeddingTest, EmbedderNegativeWrongField) try {
	constexpr static auto kNsName = "ivf_ns"sv;
	constexpr static auto kFieldNameIvf = "ivf";
	constexpr static auto kFieldNameNone = "not_yet"sv;
	constexpr static size_t kDimension = 32;
	constexpr static size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {std::string(kFieldNameNone)};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	auto err = rt.reindexer->AddIndex(kNsName, {kFieldNameIvf,
												{kFieldNameIvf},
												"float_vector",
												IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																						 .SetDimension(kDimension)
																						 .SetNCentroids(kMaxElements / 50)
																						 .SetMetric(reindexer::VectorMetric::L2)
																						 .SetEmbedding(embedding))});
	ASSERT_EQ(err.what(), "Cannot add index '" + std::string(kFieldNameIvf) + "' in namespace '" + std::string(kNsName) +
							  "'. Support for embedding only for scalar index fields. Using field '" + std::string(kFieldNameNone) +
							  "' for embedding is invalid");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, EmbedderNegativeSparceField) try {
	constexpr static auto kNsName = "ivf_ns"sv;
	constexpr static auto kFieldNameIvf = "ivf";
	constexpr static auto kFieldNameSparce = "sparce"sv;
	constexpr static size_t kDimension = 32;
	constexpr static size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {std::string(kFieldNameSparce)};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
										IndexDeclaration{kFieldNameSparce, "hash", "string", IndexOpts{}.Sparse(), 0}});
	auto err = rt.reindexer->AddIndex(kNsName, {kFieldNameIvf,
												{kFieldNameIvf},
												"float_vector",
												IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																						 .SetDimension(kDimension)
																						 .SetNCentroids(kMaxElements / 50)
																						 .SetMetric(reindexer::VectorMetric::L2)
																						 .SetEmbedding(embedding))});
	ASSERT_EQ(err.what(), "Cannot add index '" + std::string(kFieldNameIvf) + "' in namespace '" + std::string(kNsName) +
							  "'. Support for embedding only for scalar index fields. Using field '" + std::string(kFieldNameSparce) +
							  "' is sparse, so embedding is not supported");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, EmbedderNegativeCompositeField) try {
	constexpr static auto kNsName = "ivf_ns"sv;
	constexpr static auto kFieldNameIvf = "ivf";
	constexpr static auto kFieldNameFirst = "first"sv;
	constexpr static auto kFieldNameComposite = "composite"sv;
	constexpr static size_t kDimension = 32;
	constexpr static size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {std::string(kFieldNameFirst), std::string(kFieldNameComposite)};

	FloatVectorIndexOpts::EmbeddingOpts embedding;
	embedding.upsertEmbedder = embedder;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
										IndexDeclaration{kFieldNameFirst, "hash", "int", IndexOpts(), 0},
										IndexDeclaration{kFieldNameComposite, "text", "composite", IndexOpts(), 0}});
	auto err = rt.reindexer->AddIndex(kNsName, {kFieldNameIvf,
												{kFieldNameIvf},
												"float_vector",
												IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																						 .SetDimension(kDimension)
																						 .SetNCentroids(kMaxElements / 50)
																						 .SetMetric(reindexer::VectorMetric::L2)
																						 .SetEmbedding(embedding))});
	ASSERT_EQ(err.what(), "Cannot add index '" + std::string(kFieldNameIvf) + "' in namespace '" + std::string(kNsName) +
							  "'. Support for embedding only for scalar index fields. Using field '" + std::string(kFieldNameComposite) +
							  "' for embedding is invalid");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, EmbedderNegativeDropIndex) try {
	constexpr static auto kNsName = "ivf_ns"sv;
	constexpr static auto kFieldNameIvf = "ivf";
	constexpr static auto kFieldNameFirst = "first"sv;
	constexpr static auto kFieldNameSecond = "second"sv;
	constexpr static size_t kDimension = 32;
	constexpr static size_t kMaxElements = 1'000;

	FloatVectorIndexOpts::EmbedderOpts embedder;
	embedder.endpointUrl = "http://127.0.0.1:8000";
	embedder.fields = {std::string(kFieldNameFirst), std::string(kFieldNameSecond)};

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
	auto err = rt.reindexer->DropIndex(kNsName, reindexer::IndexDef{std::string(kFieldNameSecond)});
	ASSERT_EQ(err.what(), "Cannot remove index '" + std::string(kFieldNameSecond) + "': it's a part of a auto embedding logic in index '" +
							  std::string(kFieldNameIvf) + "'");
}
CATCH_AND_ASSERT

TEST_F(EmbeddingTest, SqlQuery) try {
	using namespace std::string_view_literals;
	using reindexer::Query;
	using reindexer::KnnSearchParams;
	const static std::string k1st("Pocomaxa");
	const static std::string k2nd("Hi, bro!");
	const static std::string k3rd("man`s word: blood on the asphalt");
	struct {
		Query query;
		std::string sql;
	} testData[]{{Query("ns"sv).WhereKNN("hnsw"sv, k1st, KnnSearchParams::Hnsw(4'291, 100'000)).SelectAllFields(),
				  "SELECT *, vectors() FROM ns WHERE KNN(hnsw, \'" + k1st + "\', k=4291, ef=100000)"},
				 {Query("ns"sv).WhereKNN("bf"sv, k2nd, KnnSearchParams::BruteForce(8'184)).Select("vectors()"),
				  "SELECT vectors() FROM ns WHERE KNN(bf, \'" + k2nd + "\', k=8184)"},
				 {Query("ns"sv).WhereKNN("ivf"sv, k3rd, KnnSearchParams::Ivf(823, 5)).Select({"hnsw", "vectors()"}),
				  "SELECT hnsw, vectors() FROM ns WHERE KNN(ivf, \'" + k3rd + "\', k=823, nprobe=5)"}};
	for (const auto& [query, expectedSql] : testData) {
		const auto generatedSql = query.GetSQL();
		EXPECT_EQ(generatedSql, expectedSql);
		const auto parsedQuery = Query::FromSQL(expectedSql);
		EXPECT_EQ(parsedQuery, query) << "original: " << expectedSql << "\nparsed: " << parsedQuery.GetSQL();
	}
}
CATCH_AND_ASSERT
