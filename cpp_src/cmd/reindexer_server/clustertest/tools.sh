#!/bin/bash

function CreateReplicationConf {
	local SERVER_ID=$1
	local FILE_NAME=$2

	(echo "cluster_id: 2"
		echo "server_id: $SERVER_ID"
	) > $FILE_NAME
}

function CreateClusterConf {
	local BASE_RPC_ADDRESS=$1
	local DB_NAME=$2
	local NODES_COUNT=$3
	local FILE_NAME=$4
	local node=0
	( echo "app_name: rx_node"
		echo "namespaces: []"
		echo "sync_threads: 4"
		echo "enable_compression: true"
		echo "updates_timeout_sec: 15"
		echo "retry_sync_interval_msec: 5000"
		echo "nodes:"
		while [[ $node -lt $NODES_COUNT ]]
			do
				local RPCADDR=$((BASE_RPC_ADDRESS+$node))
				echo "  -" 
				echo "    dsn: cproto://127.0.0.1:$RPCADDR/$DB_NAME"
				echo "    server_id: $node"
				node=$((node+1))
			done 
	) > "$FILE_NAME"
}

function CreateDirForClusterNodes {
	local BASE_DIR=$1
	local NODES_COUNT=$2
	local DB_NAME=$3
	local BASE_RPC_ADDRESS=$4

	rm -rf "$BASE_DIR"
	local node=0
	while [[ $node -lt $NODES_COUNT ]]
	do
		local DB_PATH="$BASE_DIR/reindex_test$(($node+1))/$DB_NAME"
		mkdir -p $DB_PATH
		local CLUSTER_CONF=$DB_PATH/cluster.conf
		local REPL_CONF=$DB_PATH/replication.conf
		echo "leveldb" > $DB_PATH/.reindexer.storage

		CreateClusterConf $BASE_RPC_ADDRESS $CLUSTER_DB_NAME $NODES_COUNT $CLUSTER_CONF

		CreateReplicationConf $node $REPL_CONF

		node=$((node+1))
	done
}

function RunServers {
	local NODES_COUNT=$1
	local BASE_RPC_ADDRESS=$2
	local BASE_HTTP_ADDRESS=$3
	local BASE_DIR=$4
	local RX_BIN_PATH=$5
	local QR_TIMEOUT=$6
	local LOG_LEVEL=$7
	local LOG_BASE_DIR=$8
	local node=0
	[ -n "$QR_TIMEOUT" ] || QR_TIMEOUT=600
	[ -n "$LOG_LEVEL" ] || LOG_LEVEL=trace
	[ -n "$LOG_BASE_DIR" ] || LOG_BASE_DIR="build/logs"
	while [[ $node -lt $NODES_COUNT ]]
	do
		HTTPADDR=$(($BASE_HTTP_ADDRESS+$node))
		RPCADDR=$(($BASE_RPC_ADDRESS+$node))
		local DB_PATH="$BASE_DIR/reindex_test$(($node+1))/$DB_NAME"
		local DB_LOG_CORE="$LOG_BASE_DIR/corelog_$node.txt"
		local DB_LOG_RPC="$LOG_BASE_DIR/rpclog_$node.txt"
		local DB_LOG_HTTP="$LOG_BASE_DIR/httplog_$node.txt"
		local DB_LOG_SERVER="$LOG_BASE_DIR/serverlog_$node.txt"
		CMD="$RX_BIN_PATH --db=$DB_PATH --loglevel=$LOG_LEVEL --serverlog=$DB_LOG_SERVER --corelog=$DB_LOG_CORE --httplog=$DB_LOG_HTTP --rpclog=$DB_LOG_RPC -p $HTTPADDR -r $RPCADDR --rpc-qr-idle-timeout $QR_TIMEOUT"
		if [ -n "$RX_PRINT_CLUSTER_STARTUP" ]; then
		  echo "Running reindexer server with cmd: '$CMD &'"
		fi
		$CMD &
		SERVER_PIDS[$node]=$!
		sleep 1
		node=$((node+1))
    done
}

function WaitForPid {
	local PID=$1
	while ps -p $PID > /dev/null; do sleep 0.1; done
}

function KillServers {
	local NODES_COUNT=${#SERVER_PIDS[@]}	
	local node=0
	while [[ $node -lt $NODES_COUNT ]]
	do
		kill ${SERVER_PIDS[$node]}
		node=$((node+1))
	done
	node=0
	while [[ $node -lt $NODES_COUNT ]]
	do
	  WaitForPid ${SERVER_PIDS[$node]}
	  node=$((node+1))
	done
}
