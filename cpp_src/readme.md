# Reindexer

**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.
Reindexer's goal is to provide fast search with complex queries.

Reindexer is compact, fast and it does not have heavy dependencies.
reindexer is up to 5x times faster, than mongodb, and 10x times than elastic search. See [benchmaks section](https://github.com/Restream/reindexer-benchmarks) for details.

# Installation

## Docker image

### Container startup

The simplest way to get reindexer, is pulling & run docker image from [dockerhub](https://hub.docker.com/r/reindexer/reindexer/)

```bash
docker run -p9088:9088 -p6534:6534 -it reindexer/reindexer
```

### Container configuration

While using docker, you may pass reindexer server config options via envinronment variables:

- `RX_DATABASE` - path to reindexer's storage. Default value is `/db`.
- `RX_CORELOG` - path to core log file (or `none` to disable core logging). Default value is `stdout`.
- `RX_HTTPLOG` - path to http log file (or `none` to disable http logging). Default value is `stdout`.
- `RX_RPCLOG` - path to rpc log file (or `none` to disable rpc logging). Default value is `stdout`.
- `RX_SERVERLOG` - path to server log file (or `none` to disable server logging). Default value is `stdout`.
- `RX_LOGLEVEL` - log level for core logs (may be `info`, `trace`, `warning` or `error`). Default value is `info`.
- `RX_PPROF` - if RX_PPROF is not empty, enables pprof api. Disabled by default.
- `RX_SECURITY` - if RX_SECURITY is not empty, enables authorization. Disabled by default.
- `RX_PROMETHEUS` - if RX_PROMETHEUS is not empty, enables prometheus metrics. Disabled by default.
- `RX_RPC_QR_IDLE_TIMEOUT` - RPC query results idle timeout (in seconds). Default value is 0 (timeout disabled).
- `RX_DISABLE_NS_LEAK` - Disables namespaces memory leak on database destruction (will slow down server's termination) 
- `RX_HTTP_READ_TIMEOUT` - if RX_HTTP_READ_TIMEOUT is not empty, sets execution timeout for HTTP read operations in seconds. 0 mean no timeout. Default value is 0.
- `RX_HTTP_WRITE_TIMEOUT` - if RX_HTTP_WRITE_TIMEOUT is not empty, sets execution timeout for HTTP write operations in seconds. 0 mean no timeout. Default value is 0 if cluster is disabled and 20 if cluster is enabled.

## Linux

### RHEL/Centos/Fedora

```bash
yum install -y epel-release yum-utils
rpm --import https://repo.reindexer.io/RX-KEY.GPG
yum-config-manager --add-repo  https://repo.reindexer.io/<distro>/x86_64/
yum update
yum install reindexer-server
```

Available distros: `centos-7`, `fedora-38`, `fedora-39`.

To install reindexer v4.x.x `reindexer-4-server` or `reindexer-4-dev` package should be used.

### Ubuntu/Debian

```bash
wget https://repo.reindexer.io/RX-KEY.GPG -O /etc/apt/trusted.gpg.d/reindexer.asc
echo "deb https://repo.reindexer.io/<distro> /" >> /etc/apt/sources.list
apt update
apt install reindexer-server
```

Available distros: `debian-bookworm`, `debian-bullseye`, `ubuntu-focal`, `ubuntu-jammy`

To install reindexer v4.x.x `reindexer-4-server` or `reindexer-4-dev` package should be used.

### Redos

```bash
rpm --import https://repo.reindexer.io/RX-KEY.GPG
dnf config-manager --add-repo https://repo.reindexer.io/<distro>/x86_64/
dnf update
dnf install reindexer-server
```

Available distros: `redos-7`.

To install reindexer v4.x.x `reindexer-4-server` or `reindexer-4-dev` package should be used.

## OSX brew

```bash
brew tap restream/reindexer
brew install reindexer
```

## Windows

Download and install [64 bit](https://repo.reindexer.io/win/64/) or [32 bit](https://repo.reindexer.io/win/32/)

## Installation from sources

### Dependencies

Reindexer's core is written in C++17 and uses LevelDB as the storage backend, so the Cmake, C++17 toolchain and LevelDB must be installed before installing Reindexer. To build Reindexer, g++ 8+, clang 5+ or MSVC 2019+ is required.
Dependencies can be installed automatically by this script:

```bash
curl -L https://github.com/Restream/reindexer/raw/master/dependencies.sh | bash -s
```

### Build & install

The typical steps for building and configuring the reindexer looks like this

```bash
git clone https://github.com/Restream/reindexer
cd reindexer
mkdir -p build && cd build
cmake ..
make -j8
# install to system
sudo make install
```

## Using reindexer server

- Start server

```
service start reindexer
```

- open in web browser http://127.0.0.1:9088/swagger to see reindexer REST API interactive documentation

- open in web browser http://127.0.0.1:9088/face to see reindexer web interface

## HTTP REST API

The simplest way to use reindexer with any program language - is using REST API. The
[complete REST API documentation is here](server/contrib/server.md).
[Or explore interactive version of Reindexer's swagger documentation](https://editor.swagger.io/?url=https://raw.githubusercontent.com/Restream/reindexer/master/cpp_src/server/contrib/server.yml)

## GRPC API

[GPRC](https://grpc.io) is a modern open-source high-performance RPC framework developed at Google that can run in any environment. It can efficiently connect services in and across data centers with pluggable support for load balancing, tracing, health checking and authentication. It uses HTTP/2 for transport, Protocol Buffers as the interface description language and it is more efficient (and also easier) to use than HTTP API. Reindexer supports GRPC API since version 3.0.

Reindexer's GRPC API is defined in [reindexer.proto](server/proto/reindexer.proto) file.

To operate with reindexer via GRPC:

1. Build reindexer_server with -DENABLE_GRPC cmake option
2. Run reindexer_server with --grpc flag
3. Build GRPC client from [reindexer.proto](server/proto/reindexer.proto) for your language https://grpc.io/docs/languages/
4. Connect your GRPC client to reindexer server running on port 16534

Pay attention to methods, that have `stream` parameters:

```protobuf
 rpc ModifyItem(stream ModifyItemRequest) returns(stream ErrorResponse) {}
 rpc SelectSql(SelectSqlRequest) returns(stream QueryResultsResponse) {}
 rpc Select(SelectRequest) returns(stream QueryResultsResponse) {}
 rpc Update(UpdateRequest) returns(stream QueryResultsResponse) {}
 rpc Delete(DeleteRequest) returns(stream QueryResultsResponse) {}
 rpc AddTxItem(stream AddTxItemRequest) returns(stream ErrorResponse) {}
```

The concept of streaming is described [here](https://grpc.io/docs/what-is-grpc/core-concepts/#server-streaming-rpc). The best example is a bulk insert operation, which is implemented via `Modify` method with mode = `INSERT`. In HTTP server it is implemented like a raw JSON document, containing all the items together, with GRPC streaming you send every item separately one by one. The second approach seems more convenient, safe, efficient and fast.

## Monitoring

### Prometheus (server-side)

Reindexer has a bunch of prometheus metrics available via http-URL `/metrics` (i.e. `http://localhost:9088/metrics`). This metrics may be enabled by passing `--prometheus` as reindexer_server command line argument or by setting `metrics:prometheus` flag in server yaml-config file. Some of the metrics also require `perfstats` to be enabled in `profiling`-config

`reindexer_qps_total` - total queries per second for each database, namespace and query type
`reindexer_avg_latency` - average queryies latency for each database, namespace and query type
`reindexer_caches_size_bytes`, `reindexer_indexes_size_bytes`, `reindexer_data_size_bytes` - caches, indexes and data size for each namespace
`reindexer_items_count` - items count in each namespace
`reindexer_memory_allocated_bytes` - current amount of dynamicly allocated memory according to tcmalloc/jemalloc
`reindexer_rpc_clients_count` - current number of RPC clients for each database
`reindexer_input_traffic_total_bytes`, `reindexer_output_traffic_total_bytes` - total input/output RPC/http traffic for each database
`reindexer_info` - generic reindexer server info (currently it's just a version number)

### Prometheus (client-side, Go)

Go binding for reindexer is using [prometheus/client_golang](https://github.com/prometheus/client_golang) to collect some metrics (RPS and request's latency) from client's side. Pass `WithPrometheusMetrics()`-option to enable metric's collecting:
```
// Create DB connection for cproto-mode with metrics enabled
db := reindexer.NewReindex("cproto://127.0.0.1:6534/testdb", reindex.WithPrometheusMetrics())
// Register prometheus handle for your HTTP-server to be able to get metrics from the outside
http.Handle("/metrics", promhttp.Handler())
```

All of the metricts will be exported into `DefaultRegistry`. Check [this](https://github.com/prometheus/client_golang/blob/main/prometheus/promauto/auto.go#L57-L85) for basic prometheus usage example.

Both server-side and client-side metrics contain 'latency', however, client-side latency will also count all the time consumed by the binding's queue, network communication (for cproto/ucproto) and deseriallization.
So client-side latency may be more rellevant for user's applications the server-side latency.

## Maintenance

For maintenance and work with data, stored in reindexer database there are 2 methods available:

- Web interface
- Command line tool

### Web interface

Reindexer server and `builtinserver` binding mode are coming with Web UI out-of-the box. To open web UI just start reindexer server
or application with `builtinserver` mode, and open http://server-ip:9088/face in browser

### Command line tool

To work with database from command line you can use reindexer [command line tool](cmd/reindexer_tool/readme.md)
Command line tool have the following functions

- Backup whole database into text file or console.
- Make queries to database
- Modify documents and DB metadata

Command line tool can run in 2 modes. With server via network, and in server-less mode, directly with storage.

### Dump and restore database

Database creation via reindexer_tool:

```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --command '\databases create mydb'
```

To dump and restore database in normal way there reindexer command line tool is used

Backup whole database into single backup file:

```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --command '\dump' --output mydb.rxdump
```

Restore database from backup file:

```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --filename mydb.rxdump
```

A bit more information about interactions between dump/restore commands and sharded namespaces may be found in [main reindexer_tool readme](cmd/reindexer_tool/readme.md)

### Replication

Reindexer supports master slave replication. To create slave DB the following command can be used:

```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/slavedb --command '\upsert #config {"type":"replication","replication":{"role":"slave","master_dsn":"cproto://127.0.0.1:6534/masterdb","cluster_id":2}}'
```

More details about replication is [here](../replication.md)

### Requests handling modes

Reindexer server supports 2 requests handling modes (those modes may be chosen independently for RPC and HTTP servers):
- "shared" (default);
- "dedicated".

Mode may be set via command line options on server startup:

```sh
reindexer_server --db /tmp/rx --rpc-threading dedicated --http-threading shared
```

In shared mode server creates fixed number of threads to handle connections (one thread per physical CPU core) and all of the connection will be distributed between those threads. In this mode requests from different connections may be forced to be executed sequentially.

In dedicated mode server creates one thread per connection. This approach may be inefficient in case of frequent reconnects or large amount of database clients (due to thread creation overhead), however it allows to reach maximum level of concurrency for requests.

## Security

Reindexer server supports login/password authorization for http/rpc client with different access levels for each user/database. To enable this feature `security` flag should be set in server.yml.
If security option is active reindexer will try to load users list from `users.yml` or `users.json`(deprecate) found in database path. If users-file was not found the default one
will be created automatically (default login/password are `reindexer`/`reindexer`)

## Alternative storages

A list of storages, which may be used by reindexer as an alternative for LevelDB.

Storage type may be selected by passing command line option to reindexer_server like this:

```sh
reindexer_server --db /tmp/rx --engine rocksdb
```

Also storage type may be set via server's `config.yml`:

```yaml
storage:
  engine: leveldb
```

To configure storage type for Go bindings either `bindings.ConnectOptions` (for builtin) or `confg.ServerConfig` (for builtinserver) structs may be used.

### RocksDB

Reindexer will try to autodetect RocksDB library and its dependencies at compile time if CMake flag `ENABLE_ROCKSDB` was passed (enabled by default).
If reindexer library was built with rocksdb, it requires Go build tag `rocksdb` in order to link with go-applications and go-bindinds.

### Data transport formats

Reindexer supports the following data formats to communicate with other applications (mainly via HTTP REST API): JSON, MSGPACK and Protobuf.

#### Protobuf

Protocol buffers are language-neutral, platform-neutral, extensible mechanism for serializing structured data. You define how you want your data to be structured once, then you can use special generated source code to easily write and read your structured data to and from a variety of data streams and using a variety of languages (https://developers.google.com/protocol-buffers).

Protocol buffers is one of the output data formats for Reindexer's HTTP REST API.

To start working with Protobuf in Reindexer you need to perform the following steps:

1.  [Set JSON Schema](server/contrib/server.md#set-namespace-schema) for all the Namespaces that you are going to use during the session.
2.  [Get text representation of Protobuf Schema](server/contrib/server.md#get-protobuf-communication-parameters-schema) (\*.proto file) that contains all the communication parameters and descriptions of the Namespaces (set in the previous step). The best practice is to enumerate all the required Namespaces at once (not to regenerate Schema one more time).
3.  Use Protobuf Schema (.proto file text representation) to generate source files to work with communication parameters in your code. In this case you usually need to write Schema data to .proto file and use `protoc` utility to generate source code files (https://developers.google.com/protocol-buffers/docs/cpptutorial#compiling-your-protocol-buffers).

To work with Protobuf as output data format you need to set `format` parameter to `protobuf` value. List of commands that support Protobuf encoding can be found in [Server documentation](server/contrib/server.md).

Example of .proto file generated by Reindexer:

```proto
// Autogenerated by reindexer server - do not edit!
syntax = "proto3";

// Message with document schema from namespace test_ns_1603265619355
message test_ns_1603265619355 {
	int64 test_3 = 3;
	int64 test_4 = 4;
	int64 test_5 = 5;
	int64 test_2 = 2;
	int64 test_1 = 1;
}
// Possible item schema variants in QueryResults or in ModifyResults
message ItemsUnion {
	oneof item { test_ns_1603265619355 test_ns_1603265619355 = 6; }
}
// The QueryResults message is schema of http API methods response:
// - GET api/v1/db/:db/namespaces/:ns/items
// - GET/POST api/v1/db/:db/query
// - GET/POST api/v1/db/:db/sqlquery
message QueryResults {
	repeated ItemsUnion items = 1;
	repeated string namespaces = 2;
	bool cache_enabled = 3;
	string explain = 4;
	int64 total_items = 5;
	int64 query_total_items = 6;
	message Columns {
		string name = 1;
		double width_percents = 2;
		int64 max_chars = 3;
		int64 width_chars = 4;
	}
	repeated Columns columns = 7;
	message AggregationResults {
		double value = 1;
		string type = 2;
		message Facets {
			int64 count = 1;
			repeated string values = 2;
		}
		repeated Facets facets = 3;
		repeated string distincts = 4;
		repeated string fields = 5;
	}
	repeated AggregationResults aggregations = 8;
}
// The ModifyResults message is schema of http API methods response:
// - PUT/POST/DELETE api/v1/db/:db/namespaces/:ns/items
message ModifyResults {
	repeated ItemsUnion items = 1;
	int64 updated = 2;
	bool success = 3;
}
// The ErrorResponse message is schema of http API methods response on error condition
// With non 200 http status code
message ErrorResponse {
	bool success = 1;
	int64 response_code = 2;
	string description = 3;
}
```

In this example JSON Schema was set for the only one Namespace: test_ns_1603265619355. Pay attention to message `ItemsUnion` described like this:

```proto
message ItemsUnion {
	oneof item { test_ns_1603265619355 test_ns_1603265619355 = 6; }
}
```

In case if JSON Schema was set not only for `test_ns_1603265619355` but for several other namespaces this message should have described all of them.
e.g.

```proto
message ItemsUnion {
	oneof item {
		namespace1 namespace1 = 1;
		namespace2 namespace2 = 2;
		namespace3 namespace3 = 3;
	}
}
```

Field `items` in `QueryResults`

```proto
repeated ItemsUnion items = 1;
```

contains Query execution result set. To get results of an appropriate type (type of requested Namespace) you need to work with `oneof` message, documentation for your language can be found here (https://developers.google.com/protocol-buffers/docs/proto#oneof). In case of Python it looks like this:

```python
for it in queryresults.items:
   item = getattr(it, it.WhichOneof('item'))
```

or like this:

```python
for it in queryresults.items:
   item = getattr(it, self.namespaceName)
```

where both `self.namespaceName` and `it.WhichOneof('item')` represent a name of the requested namespace.

# Optional dependencies

- `Doxygen` package is also required for building a documentation of the project.
- `gtest`,`gbenchmark` for run C++ tests and benchmarks
- `gperftools` for memory and performance profiling
