#!/bin/bash

# Sharding scheme for namespace 'ns'. Sharding key is 'location'
#
#                    shard0 (default)
#            (all other values for "location")
#               /                     \
#              /                       \
#            shard1------------------shard2
#     ("Europe", "Asia")        ("North America")
#

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
	echo '          - "Europe"'
	echo '          - "Asia"'
	echo '      - shard_id: 2'
	echo '        values:'
	echo '          - "North America"'
	echo 'shards:'
	echo '  - shard_id: 0'
	echo '    dsns:'
	echo '      - "cproto://127.0.0.1:6110/'$DB'"'
	echo '  - shard_id: 1'
	echo '    dsns:'
	echo '      - "cproto://127.0.0.1:6210/'$DB'"'
	echo '  - shard_id: 2'
	echo '    dsns:'
	echo '      - "cproto://127.0.0.1:6310/'$DB'"'
	echo "this_shard_id: $CURRENT_SHARD_ID") > $FILE_CONF
}

DB_NAME=sharding_db

# Clean up directory for nodes
rm -rf "/tmp/rx_sharding_preview"

SHARD_COUNT=3
REINDEXER_SERVER_BINARY="reindexer_server"

shard=0

LOG_BASE="/tmp/rx_sharding_preview/logs"
while [[ $shard -lt $SHARD_COUNT ]]
do
	DB_PATH_BASE="/tmp/rx_sharding_preview/cluster_shard$(($shard+1))/"
	DB_PATH="$DB_PATH_BASE$DB_NAME"
	mkdir -p $DB_PATH
	REPL_CONF=$DB_PATH/replication.conf
	SHARD_CONF=$DB_PATH/sharding.conf
	echo "leveldb" > $DB_PATH/.reindexer.storage

	RPCADDR_BASE=$((6110+100*$shard))
	echo "RPCADDR_BASE = $RPCADDR_BASE"

	
	CreateReplicationConf $shard $REPL_CONF

	CreateShardConf $DB_NAME $shard $SHARD_CONF

	DB_LOG_CORE="$LOG_BASE/corelog-$shard.txt"
	DB_LOG_RPC="$LOG_BASE/rpclog_$shard.txt"
	DB_LOG_HTTP="$LOG_BASE/httplog_$shard.txt"
	DB_LOG_SERVER="$LOG_BASE/serverlog_$shard.txt"
	RPCADDR=$(($RPCADDR_BASE))
	HTTPADDR=$(($RPCADDR+1000))
	echo "HTTP=$HTTPADDR, RPC=$RPCADDR"
	$REINDEXER_SERVER_BINARY --db=$DB_PATH_BASE --loglevel=trace --serverlog="" --corelog=$DB_LOG_CORE --httplog=$DB_LOG_HTTP --rpclog=$DB_LOG_RPC -p $HTTPADDR -r $RPCADDR &
			
	shard=$((shard+1))
done
