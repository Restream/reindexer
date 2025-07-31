<!-- toc -->

- [Creation](#creation)
  * [HNSW options](#hnsw-options)
  * [IVF options](#ivf-options)
  * [Embedding configuration](#embedding-configuration)
  * [Embedding cache configuration](#embedding-cache-configuration)
- [Float vector fields in selection results](#float-vector-fields-in-selection-results)
- [KNN search](#knn-search)
- [KNN search with auto-embedding](#knn-search-with-auto-embedding)
- [Rank](#rank)
- [Query examples](#query-examples)
- [Environment variables affecting vector indexes](#environment-variables-affecting-vector-indexes)
- [Additional action commands](#additional-action-commands)
  * [Rebuilding clusters for IVF index](#rebuilding-clusters-for-ivf-index)
  * [Removing disk cache for ANN indexes](#removing-disk-cache-for-ann-indexes)
  * [Create embedding for existing documents](#create-embedding-for-existing-documents)
  * [Removing disk cache for embedders](#removing-disk-cache-for-embedders)

<!-- tocstop -->

## Creation
Reindexer supports three types of vector indexes: `hnsw` (based on [HNSWlib](https://github.com/nmslib/hnswlib)), `ivf` (based on [FAISS IVF FLAT](https://github.com/facebookresearch/faiss)) and brute force (`vec_bf`),
and three types of metrics for calculating the measure of similarity of vectors: `inner_product`, `l2` and `cosine`.

For all types of vector indexes, the `metric` and the `dimension` should be explicitly specified.
Vectors of only the specified dimension can be inserted and searched in the index.

Optionally `radius` could be specified for filtering vectors by metric.

The initial size `start_size` can optionally be specified for `brute force` и `hnsw` indexes, which helps to avoid reallocation and reindexing.
The optimal value is equal to the size of the fully filled index.
A much larger `start_size` value will result in memory overuse, while a much smaller `start_size` value will slow down inserts.
Minimum and default values are 1000.

Automatic embedding of vector indexes is also supported. It is expected that the vector generation service is configured.
Its URL is needed, and the basic index fields are specified. Contents of the base fields are passed to the service, and the service returns the calculated vector value.

### HNSW options
`Hnsw` index is also configured with the following parameters:
- `m` - https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md#construction-parameters.
Optional, range of values is [2, 128], default value is 16.
- `ef_construction` - https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md#construction-parameters.
Optional, range of values is [4, 1024], default value is 200.
- `multithreading` - 0 or 1. 0 - single thread insert mode is used (uses less memory and CPU, but inserting into the index is slower). 1 - multithread insertion mode is used for transactions, but single inserts are still single thread (the index itself takes up more memory, but has a high parallelism factor).
The number of insertion threads is specified via `tx_vec_insertion_threads` in namespace `#config`, by default 4 threads.\
Optional, default value is 0.

### IVF options
For `ivf` index, the number of vectors (called centroids) should be specified, that will be chosen to partition the entire set of vectors into clusters.
Each vector belongs to the cluster of the centroid closest to itself.
The higher the `centroids_count`, the fewer vectors each cluster contains, this will speed up the search but will slow down the index building.\
Required, range of values is [1, 65536].
It is recommended to take values of the order between $4 * \sqrt{number\: of\: vectors\: in\: the\: index}$ and $16 * \sqrt{number\: of\: vectors\: in\: the\: index}$.

### Examples
```go
// For a vector field, the data type must be array or slice of `float32`.
type Item struct {
	Id      int           `reindex:"id,,pk"`
	// In case of a slice, `dimension` should be explicitly specified in the field tag.
	VecBF   []float32     `reindex:"vec_bf,vec_bf,start_size=1000,metric=l2,dimension=1000"`
	// In case of an array, `dimension` is taken to be equal to the size of the array.
	VecHnsw [2048]float32 `reindex:"vec_hnsw,hnsw,m=16,ef_construction=200,start_size=1000,metric=inner_product,multithreading=1"`
	VecIvf  [1024]float32 `reindex:"vec_ivf,ivf,centroids_count=80,metric=cosine,radius=0.5"`
}
```

When adding a vector index to an existing namespace, the field on which the index will be built must be empty or contain an array of numbers of length equal to the `dimension` of the index.
```go
ivfOpts := reindexer.FloatVectorIndexOpts {
	Metric:         "l2",
	Dimension:      1024,
	CentroidsCount: 32,
	Radius:         1e20,
}
indexDef := reindexer.IndexDef {
	Name:       "vec",
	JSONPaths:  []string{"vec"},
	IndexType:  "ivf",
	FieldType:  "float_vector",
	Config:     ivfOpts,
}
err := DB.AddIndex("ns_name", indexDef)
if err != nil {
	panic(err)
}
```

### Embedding configuration
Reindexer is able to perform automatic remote HTTP API calls to receive embedding for documents' fields or strings in KNN queries conditions. Currently, reindexer's core simply sends fields/conditions content to external user's service and expects to receive embedding results.

Embedding service has to implement this [openapi spec](embedders_api.yaml).

> Notice: current embedding callback API is in beta and may be changed in the next releases

To configure automatic embedding you should set `config` field in the target vector index:

```json
"config": {
  "embedding": {
    "upsert_embedder": {
      "name": <Embedder name>
      "URL": <URL service>,
      "cache_tag": <name, used to access the cache>,
      "fields": [ "idx1", "idx2" ]
      "embedding_strategy": <"always"|"empty_only"|"strict">
      "pool": {
        "connections": 10,
        "connect_timeout_ms": 300,
        "read_timeout_ms": 5000,
        "write_timeout_ms": 5000
      }
    },
    "query_embedder": {
      "name": <Embedder name>
      "URL": <URL service>,
      "cache_tag": <name, used to access the cache>,
      "pool": {
        "connections": 10,
        "connect_timeout_ms": 300,
        "read_timeout_ms": 5000,
        "write_timeout_ms": 5000
      }
    }
  }
}
```
- `name` - Embedder name. Optional. If not set, default generation logic is used, in lower case: <NS_NAME>_<INDEX_NAME>
- `URL` - Embed service URL. The address of the service where embedding requests will be sent. Required
- `cache_tag` - Name, used to access the cache. Optional, if not specified, caching is not used
- `fields` - List of index fields to calculate embedding for. Required. Sparse or composite fields are not supported. Don't use fields with precept (may produce incorrect results)
- `embedding_strategy` - Embedding injection strategy. Optional
  + `always` - Default value, always embed
  + `empty_only` - When the user specified any value for the embedded field (non-empty vector), then automatic embedding is not performed
  + `strict` - When the user sets some value for the embedded field (non-empty vector), we return an error. If the field is empty, we automatically embed
- `pool` - Connection pool configuration

It is also optionally possible to configure a connection `pool`:
  + `connections` - Number connections to service. Optional, [1..1024], default 10
  + `connect_timeout_ms` - Connection\reconnection timeout to any embedding service (milliseconds). Optional, minimum 100, default 300
  + `read_timeout_ms` - Timeout reading data from embedding service (milliseconds). Optional, minimum 500, default 5000
  + `write_timeout_ms` - Timeout writing data from embedding service (milliseconds). Optional, minimum 500, default 5000

Upsert embedder used in Insert/Update/Upsert operations, send format is json: /api/v1/embedder/*NAME*/produce?format=json.
Query embedder starts with `WhereKNN`, sending a string as the search value (?format=text).
The embedding process: sends JSON values for all fields involved in the embedding to the specified URL.
For one requested vector:
```json
{"data":[{"field0":val0, "field1":val1, "field2":[val20, val21, ...], ...}]
```
Or a batch for several:
```json
{"data":[{"field0":val0, "field1":[val10, val11], ...}, ..., {"field0":val0, "field1":[], ...}]}
```
Query embedder format for single and for package for multiple:
```json
{"data":["WhereKNN input search text", ...]}
```
For information on request/response formats, [openapi spec](embedders_api.yaml).
As a response, the produce should always return an array of arrays of objects to support subsequent chunking:
```json
[
  "_comment": "One array corresponds to one object/string that came to produce",
  [
    "_comment": "One such object corresponds to one chunk. At this stage, there should always be one chunk, and the data from the 'chunk' itself will be ignored - only the vector from the embedding field will be used",
    {
      "chunk": "some data",
      "embedding": [ 1.1, 0.7, ...]
    },
    {
      "chunk": "more data",
      "embedding": [ 0.1, -1.0, ...]
    },
    ...
  ],
  ...
]
```

- Go
```go
connectConfig := &bindings.EmbedderConnectionPoolConfig{
	Connections:    3,
	ConnectTimeout: 500,
	ReadTimeout:    500,
	WriteTimeout:   500,
}
embedderConfig := &bindings.EmbedderConfig{
	URL:                  "http://127.0.0.1:8000",
	Fields:               []string{"name", "value"},
	CacheTag:             "HNSW",
	EmbeddingStrategy:    "always",
	ConnectionPoolConfig: connectConfig,
}
embeddingConfig := &bindings.EmbeddingConfig{
	UpsertEmbedder: embedderConfig,
}
hnswSTOpts := reindexer.FloatVectorIndexOpts{
	Metric:             "inner_product",
	Dimension:          1024,
	M:                  8,
	EfConstruction:     100,
	StartSize:          256,
	MultithreadingMode: 1,
	EmbeddingConfig:    embeddingConfig,
}
indexDef := reindexer.IndexDef{
	Name:      "vec",
	JSONPaths: []string{"vec"},
	IndexType: "hnsw",
	FieldType: "float_vector",
	Config:    hnswSTOpts,
}
err := DB.AddIndex("embedding_hnws", indexDef)
if err != nil {
	panic(err)
}
```

### Embedding cache configuration
When do embedding, it makes sense to use result caching, which can improve performance. To do this, you need to configure it.
Setting up embedding caches is done in two places. Firstly, `cache_tag` parameter that was described above.
`cache_tag` is part of `config` field in the target vector index description. `cache_tag` it's simple name\identifier,
used to access the cache. Optional, if not specified, caching is not used. The name may not be unique.
In this case, different embedders can put the result in the same cache. But keep in mind that this only works well
if the source data for the embedder does not overlap. Or if the embedders return exactly the same values for the same request.
Secondly, special item in system `#config` namespace, with type `embedders`. Optional, if not specified, caching is not used for all embedders.
```json
{
  "type":"embedders",
  "caches":[
    {
      "cache_tag":"*",
      "max_cache_items":1000000,
      "hit_to_cache":1
    },
    {
      "cache_tag":"the jungle book",
      "max_cache_items":2025,
      "hit_to_cache":3
    }
  ]
}
```
- SQL
```sql
UPDATE #config set caches=[{"cache_tag":"*","max_cache_items":1000000,"hit_to_cache":1},{"cache_tag":"the jungle book","max_cache_items":2025,"hit_to_cache":3}] where type='embedders'
```
- `cache_tag` - Name, used to access the cache. Required. The special character `*` can be used, in which case the settings apply to all configured embedders with not empty `cache_tag`.
Provided there is no specialization. The specialization is determined by the match of the `cache_tag` values in this configuration and for the embedder (like `the jungle book`).
- `max_cache_items` - Maximum size of the embedding results cache in items. This cache will only be enabled if the `max_cache_items` property is not `off` (value `0`).
It stores the results of the embedding calculation. Minimum `0`, default `1000000`.
- `hit_to_cache` - This value determines how many requests required to put results into cache. For example with value of `2`: first request will be executed without caching,
second request will generate cache entry and put results into the cache and third request will get cached results. `0` and `1` mean - when value added goes straight to the cache.
Minimum `0`, default `1`.
- GO
```go
nsConfig := make([]reindexer.DBEmbeddersConfig, 1)
nsConfig[0].CacheTag = "*"
nsConfig[0].MaxCacheItems = 10000
nsConfig[0].HitToCache = 0
item := reindexer.DBConfigItem{
	Type:      "caches",
	Embedders: &nsConfig,
}
err := DB.Upsert(reindexer.ConfigNamespaceName, item)
if err != nil {
	panic(err)
}
```
- SQL
```SQL
update #config set caches[0] = {"cache_tag":"*", "max_cache_items":10000, "hit_to_cache":0} where type='embedders'
```
Or you can update one parameter
```SQL
update #config set caches[0].max_cache_items = 4 where type='embedders'
```

## Float vector fields in selection results
By default, float vector fields are excluded from the results of all queries to namespaces containing vector indexes.
If you need to get float vector fields, you should specify this explicitly in the query.
Either by listing the required fields, or by requesting the output of all `vectors()`.

- By default, without explicitly requesting vector fields:
```sql
SELECT * FROM test_ns;
```
```go
db.Query("test_ns")
db.Query("test_ns").Select("*")
```
Result:
```json
{"id": 0}
```
- Requesting for specific fields:
```sql
SELECT *, VecBF FROM test_ns;
```
```go
db.Query("test_ns").Select("*", "VecBF")
```
Result:
```json
{"id": 0, "vec_bf": [0.0, 1.1, ...]}
```
- Requesting all vector fields:
```sql
SELECT *, vectors() FROM test_ns;
```
```go
db.Query("test_ns").Select("*", "vectors()")
db.Query("test_ns").SelectAllFields()
```
Result:
```json
{"id": 0, "vec_bf": [0.0, 1.1, ...], "vec_hnsw": [1.2, 3.5, ...], "vec_ivf": [5.1, 4.7, ...]}
```

## KNN search
Supported filtering operations on floating-point vector fields are `KNN`, `Empty`, and `Any`.
It is not possible to use multiple `KNN` filters in a query, and it is impossible to combine filtering by `KNN` and fulltext.

Parameters set for `KNN`-query depends on the specific index type. It is required to specify `k` or `radius` (or both) for every index type. `k` is the maximum number of documents returned from the index for subsequent filtering.

In addition to the parameter `k` (or instead it), the query results can also be filtered by a `rank`-value using the parameter `radius`. It's named so because, under the `L2`-metric, it restricts vectors from query result to a sphere of the specified radius.

*Note: To avoid confusion, in case of `L2`-metric, for performance optimization, the ranks of vectors in the query result are actually squared distances to the query vector. Thus, while the parameter is called `radius`, the passed value is interpreted as the squared distance.*

- For the `L2`-metric, vectors are discarded if their ranks are greater than the `radius`-parameter value.
- For `cosine` and `inner product` metrics, the logic is reversed: vectors with rank-values less than the threshold are discarded. The `radius`-parameter can also take negative values for these metrics.
Both parameters (`k` and `radius`) can be specified together, or only one of them may be used.

When searching by `hnsw` index, you can additionally specify the `ef` parameter.
Increasing this parameter allows you to get a higher quality result (recall rate), but at the same time slows down the search.
See description [here](https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md#search-parameters).
Optional, minimum and default values are `k`.

When searching by `ivf` index, you can additionally specify the `nprobe` parameter - the number of clusters to be looked at during the search.
Increasing this parameter allows you to get a higher quality result (recall rate), but at the same time slows down the search.
Optional, should be greater than 0, default value is 1.

- SQL\
KNN search condition is written as a function `KNN`, which takes the vector field as its first argument, the desired vector as its second argument, and `k`, `ef`, and `nprobe` as named arguments at the end:
```sql
SELECT * FROM test_ns WHERE KNN(vec_bf, [2.4, 3.5, ...], k=100, radius=1.23)
SELECT * FROM test_ns WHERE KNN(vec_hnsw, [2.4, 3.5, ...], k=100, radius=-1.23, ef=200)
SELECT * FROM test_ns WHERE KNN(vec_ivf, [2.4, 3.5, ...], k=100, nprobe=10)
```
- Go\
The `knn` query parameters are passed in the `reindexer.BaseKnnSearchParam` structure, or in specific structures for each index type: `reindexer.IndexBFSearchParam`, `reindexer.IndexHnswSearchParam` or `reindexer.IndexIvfSearchParam`.
There are factory methods for constructing them:
```go
func NewIndexBFSearchParam(baseParam BaseKnnSearchParam) (*IndexBFSearchParam, error)
func NewIndexHnswSearchParam(ef int, baseParam BaseKnnSearchParam) (*IndexHnswSearchParam, error)
func NewIndexIvfSearchParam(nprobe int, baseParam BaseKnnSearchParam) (*IndexIvfSearchParam, error)
```
```go
knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(4291)
// knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(4291).SetRadius(31.2)
// knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetRadius(31.2)
// brute force
db.Query("test_ns").WhereKnn("vec_bf", []float32{2.4, 3.5, ...}, knnBaseSearchParams)
// hnsw
hnswSearchParams, err := reindexer.NewIndexHnswSearchParam(100000, knnBaseSearchParams)
if err != nil {
	panic(err)
}
db.Query("test_ns").WhereKnn("vec_hnsw", []float32{2.4, 3.5, ...}, hnswSearchParams)
// ivf
ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(10, knnBaseSearchParams)
if err != nil {
	panic(err)
}
db.Query("test_ns").WhereKnn("vec_ivf", []float32{2.4, 3.5, ...}, ivfSearchParams)
```

## KNN search with auto-embedding
KNN search with automatic embedding works much like simple KNN search.
With one exception, it expects a string instead of a vector. It then sends that string to the embedding service, gets back a calculated vector.
And that vector is then used as the actual value for filtering the database.
Before this need to configure "query_embedder".

- SQL\
  All queries are similar to KNN search
```sql
SELECT * FROM test_ns WHERE KNN(vec_bf, '<text to embed calculate>', k=100)
SELECT * FROM test_ns WHERE KNN(vec_hnsw, '<text to embed calculate>', k=100, ef=200)
SELECT * FROM test_ns WHERE KNN(vec_ivf, '<text to embed calculate>', k=100, nprobe=10)
```
- Go
```go
knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(4291)
// brute force
it := db.Query("test_ns").WhereKnnString("vec_bf", "<text to embed calculate>", knnBaseSearchParams).Exec()
defer it.Close()

// hnsw
hnswSearchParams, err := reindexer.NewIndexHnswSearchParam(100000, knnBaseSearchParams)
if err != nil {
    panic(err)
}
it := db.Query("test_ns").WhereKnnString("vec_hnsw", "<text to embed calculate>", hnswSearchParams).Exec()
defer it.Close()

// ivf
ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(10, knnBaseSearchParams)
if err != nil {
    panic(err)
}
it := db.Query("test_ns").WhereKnnString("vec_ivf", "<text to embed calculate>", ivfSearchParams).Exec()
defer it.Close()
```
-HTTP
```http request
curl --location --request POST 'http://127.0.0.1:9088/api/v1/db/vectors_db/query' \
--header 'Content-Type: application/json' \
--data-raw '{
  "namespace": "test_ns",
  "type": "select",
  "filters": [
    {
      "op": "and",
      "cond": "knn",
      "field": "vec_bf",
      "value": "<text to embed calculate>",
      "params": {"k": 100}
    }
  ],
}'
```

## Rank
By default, the results of queries with KNN are sorted by `rank`, that is equal to requested metric values.
For indexes with the `l2` metric, from a lower value to a higher value, and with the `inner_product` and `cosine` metrics, from a higher value to a lower value.
This is consistent with the best match for the specified metrics.

When it is necessary to get the `rank`-value of each document in the query result, it must be requested explicitly via the `RANK()` function in SQL or `WithRank()` in GO:
```sql
SELECT *, RANK() FROM test_ns WHERE KNN(vec_bf, [2.4, 3.5, ...], k=200)
```
```go
knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(200)
db.Query("test_ns").WithRank().WhereKnn("vec_bf", []float32{2.4, 3.5, ...}, knnBaseSearchParams)
```
Result:
```json
{"id": 0, "rank()": 1.245}
```
`rank` can also be used to sort by expression. Described in detail [here](readme.md/#sort).

## Query examples
- Simple `KNN` query on `hnsw` index:
```sql
SELECT * FROM test_ns WHERE KNN(vec_hnsw, [2.4, 3.5, ...], k=100, ef=150)
```
```go
knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(100)
hnswSearchParams, err := reindexer.NewIndexHnswSearchParam(150, knnBaseSearchParams)
if err != nil {
	panic(err)
}
db.Query("test_ns").WhereKnn("vec_hnsw", []float32{2.4, 3.5, ...}, hnswSearchParams)
```
- Simple `KNN` query on `brute force` index with `rank` requesting:
```sql
SELECT *, RANK() FROM test_ns WHERE KNN(vec_bf, [2.4, 3.5, ...], k=100)
```
```go
knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(100)
db.Query("test_ns").
	WithRank().
	WhereKnn("vec_bf", []float32{2.4, 3.5, ...}, knnBaseSearchParams)
```
- Simple `KNN` query on `ivf` index with requesting of vector fields:
```sql
SELECT *, vectors() FROM test_ns WHERE KNN(vec_ivf, [2.4, 3.5, ...], k=200, nprobe=32)
```
```go
knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(100)
ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(32, knnBaseSearchParams)
if err != nil {
	panic(err)
}
db.Query("test_ns").
	SelectAllFields().
	WhereKnn("vec_ivf", []float32{2.4, 3.5, ...}, ivfSearchParams)
```
- `id` > 5 `AND` `KNN` query on `ivf` index with requesting one vector field:
```sql
SELECT *, VecBF FROM test_ns WHERE id > 5 AND KNN(vec_ivf, [2.4, 3.5, ...], k=300, nprobe=32)
```
```go
knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(300)
ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(32, knnBaseSearchParams)
if err != nil {
	panic(err)
}
db.Query("test_ns").
	Select("*", "VecBF").
	Where("id", reindexer.GT, 5).
	WhereKnn("vec_ivf", []float32{2.4, 3.5, ...}, ivfSearchParams)
```
- Query with parentheses and ordering by expression with `rank`:
```sql
SELECT * FROM test_ns
WHERE
	id < 1000
	AND (
		id > 5
		AND KNN(vec_ivf, [2.4, 3.5, ...], k=10000, nprobe=32)
	)
ORDER BY 'rank() * 10 - id' DESC
```
```go
knnBaseSearchParams := reindexer.BaseKnnSearchParam{}.SetK(10000)
ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(32, knnBaseSearchParams)
if err != nil {
	panic(err)
}
db.Query("test_ns").
	Where("id", reindexer.LT, 1000).
	OpenBracket().
		Where("id", reindexer.GT, 5).
		WhereKnn("vec_ivf", []float32{2.4, 3.5, ...}, ivfSearchParams).
	CloseBracket().
	Sort("rank() * 10 - id", true)
```

## Environment variables affecting vector indexes
- `RX_IVF_MT` -  0 or 1. If it is 1, then for `IVF` indexes a multithread search mode is used. Default value is 0.
- `RX_IVF_OMP_THREADS` - number of OpenMP threads, used during constructing clusters for `IVF` indices. Default value is 8.
- `RX_DISABLE_ANN_CACHE` - if set, reindexer turns off disk ANN-cache to build vector indexes when loading data from disk during startup (it is not recommended to turn off the cache, as it slows down startup).
- `RX_CUSTOM_BLAS_LIB_NAME` - allows to set a custom name for BLAS library, used in `IVF` indexes (for cases when reindexer cannot find it on its own).
- `RX_CUSTOM_LAPACK_LIB_NAME` - allows to set a custom name for LAPACK library, used in `IVF` indexes (for cases when reindexer cannot find it on its own).
- `RX_TARGET_INSTRUCTIONS` - expects one of the values `avx512`, `avx2`, `avx` or `sse`.
Allows you to select a set of vector instructions used for distance calculation functions.
By default, if it is not set, reindexer uses the "best" available for the CPU it is running on, but occasionally `avx512` may be less effective than `avx2`/`avx` in multithread environment.

## Additional action commands
These commands can be used by inserting them via upsert into the #config namespace.

### Rebuilding clusters for IVF index
```json
{"type":"action","action":{"command":"rebuild_ivf_index", "namespace":"*", "index":"*", "data_part": 0.5}}
```
The command can be useful for cases where the composition of vectors in the index has changed significantly and the current centroids do not provide sufficiently high-quality output.
- `namespace` - the target namespace (`*` - applies the command to all namespaces);
- `index` - the target index (`*` - applies the command to all suitable indexes in namespace);
- `data_part` - the portion of the total data in the index that will be used to build new clusters (range [0.0, 1.0]). The more data is used, the more accurate the resulting clusters will be, but the computation time for the centroids will also be longer.

### Removing disk cache for ANN indexes
```json
{"type":"action","action":{"command":"drop_ann_storage_cache", "namespace":"*", "index":"*"}}
```
The command can be useful for cases when you need to force the re-creation of the disk cache for ANN indexes, or disable it completely (using it together with the `RX_DISABLE_ANN_CACHE` environment variable).
- `namespace` - the target namespace (`*` - applies the command to all namespaces);
- `index` - the target index (`*` - applies the command to all suitable indexes in namespace).

### Create embedding for existing documents
- JSON
```json
{"type":"action","action":{"command":"create_embeddings", "namespace":"*", "batch_size":100}}
```
- SQL
```sql
UPDATE #config set action={ "command": "create_embeddings", "namespace": "*", "batch_size": 100} where type='action'
```
- Go
```go
err := DB.Upsert("#config", []byte("{\"type\":\"action\",\"action\":{\"command\":\"create_embeddings\", \"namespace\":\"*\", \"batch_size\":100}}"))
```
The command can be useful in cases when you need to add embedded values to existing documents after setting up automatic embedding
or to update existing values when changing the embedder. The update occurs for all embedders in the namespace.
You need to be careful and pay close attention to the settings of the strategy for updating embedders.
The 'strict' strategy does not make sense at this stage. If documents contain embedded values, but there are also documents without filled vectors,
it makes sense to use the 'empty_only' strategy. If you need to replace all values in a field with configured auto-embed,
then you should use the 'always' strategy. The strategy is an embedder setting and can be set individually ("config": {
"embedding": { "upsert_embedder": { "embedding_strategy": ...). This action only locally applied
- `namespace` - the target namespace (`*` - applies the command to all namespaces);
- `batch_size` - number, how many documents to update in the namespace at a time (batch size in transaction). Optional parameter.

### Removing disk cache for embedders
- JSON
```json
{"type":"action","action":{"command":"clear_embedders_cache", "cache_tag":"*"}}
```
- SQL
```sql
UPDATE #config set action={ "command": "clear_embedders_cache", "cache_tag": "*" } where type='action'
```
- Go
```go
err := DB.Upsert("#config", []byte("{\"type\":\"action\",\"action\":{\"command\":\"clear_embedders_cache\", \"cache_tag\":\"*\"}}"))
```
The command can be useful in cases where it is necessary to reset cached content for all or a specific embedder
- `cache_tag` - the target tag (`*` - applies the command to all matching embedder caches in the namespace).
