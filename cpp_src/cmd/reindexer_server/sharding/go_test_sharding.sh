#!/bin/bash

set -e

function CreateReplicationConf {
	local SERVER_ID=$1
	local FILE_CONF=$2

	(echo "cluster_id: 2"
		echo "server_id: $SERVER_ID"
	) > $FILE_CONF

}

function CreateShardConf {
	local DB=$1
	local CURRENT_SHARD_ID=$2
	local FILE_CONF=$3

	( echo 'version: 1' 
	echo 'namespaces:'	
	echo '  - namespace: "ns"'
	echo '    default_shard: 0'
	echo '    index: "location"'
	echo '    keys:'	
	echo '      - shard_id: 1'
	echo '        values:'
	echo '          - "key1"'
	echo '      - shard_id: 2'
	echo '        values:'
	echo '          - "key2"'
	echo 'shards:'
	echo '  - shard_id: 0'
	echo '    dsns:'
	echo '      - "cproto://127.0.0.1:6000/'$DB'"'
	echo '  - shard_id: 1'
	echo '    dsns:'
	echo '      - "cproto://127.0.0.1:6100/'$DB'"'
	echo '  - shard_id: 2'
	echo '    dsns:'
	echo '      - "cproto://127.0.0.1:6200/'$DB'"'
	echo "this_shard_id: $CURRENT_SHARD_ID") > $FILE_CONF
}

DB_NAME=sharding_db

# Clear directories for nodes
DB_TMP_DIR="/tmp"
if [[ -n $REINDEXER_TEST_DB_ROOT ]]; then
	DB_TMP_DIR=$REINDEXER_TEST_DB_ROOT
fi
DB_COMMON_PATH_PREFIX="$DB_TMP_DIR/rx_shard_only"
rm -rf "$DB_COMMON_PATH_PREFIX"

LOG_BASE_DIR="build/logs"
rm -rf $LOG_BASE_DIR

SHARD_COUNT=3
shard=0

while [[ $shard -lt $SHARD_COUNT ]]
do
	DB_PATH_BASE="$DB_COMMON_PATH_PREFIX/cluster_shard$(($shard+1))/"
	DB_PATH="$DB_PATH_BASE$DB_NAME"
	mkdir -p $DB_PATH
	REPL_CONF=$DB_PATH/replication.conf
	SHARD_CONF=$DB_PATH/sharding.conf
	echo "leveldb" > $DB_PATH/.reindexer.storage

	RPCADDR_BASE=$((6000+100*$shard))
	
	CreateReplicationConf $shard $REPL_CONF

	CreateShardConf $DB_NAME $shard $SHARD_CONF

	DB_LOG_CORE="$LOG_BASE_DIR/corelog-$shard.txt"
	DB_LOG_RPC="$LOG_BASE_DIR/rpclog_$shard.txt"
	DB_LOG_HTTP="$LOG_BASE_DIR/httplog_$shard.txt"
	DB_LOG_SERVER="$LOG_BASE_DIR/serverlog_$shard.txt"
	RPCADDR=$(($RPCADDR_BASE))
	HTTPADDR=$(($RPCADDR+1000))
	build/cpp_src/cmd/reindexer_server/reindexer_server --db=$DB_PATH_BASE --loglevel=trace --serverlog="" --corelog=$DB_LOG_CORE --httplog=$DB_LOG_HTTP --rpclog=$DB_LOG_RPC -p $HTTPADDR -r $RPCADDR &
	SERVER_PID[$shard]=$!	
	shard=$((shard+1))
done	

mkdir gotests || true
# Using preocompiled tests to get readable sanitizers backtraces from builtin C++ library
go test ./test/sharding/... -c -o gotests/sharding.test -tags sharding_test
./gotests/sharding.test -test.run "TestShardingIDs|TestShardingBuiltin" -dsn cproto://127.0.0.1:6000/sharding_db -test.count=1

# Kill servers
node=0
while [[ $node -lt $SHARD_COUNT ]]
do
	kill ${SERVER_PID[$node]}
	wait ${SERVER_PID[$node]}
	node=$((node+1))
done
