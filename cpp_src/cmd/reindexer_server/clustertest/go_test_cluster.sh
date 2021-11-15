#!/bin/bash

set -e

while test $# -gt 0; do
	case "$1" in
		-n)
		shift
		if test $# -gt 0; then
			NODES_NUMBER=$1
		else
			echo "no nodes count specified"
		fi
		shift
		;;
		*)
			echo "unknown param"
				shift
				;;
	esac
done

CLUSTER_DB_NAME=cluster_db

# Create directories for nodes
rm -rf "/tmp/rx_gocluster"
LOG_BASE_DIR="build/logs"
rm -rf $LOG_BASE_DIR
node=0
while [[ $node -lt $NODES_NUMBER ]]
do
	DB_PATH="/tmp/rx_gocluster/reindex_test$(($node+1))/$CLUSTER_DB_NAME"
	mkdir -p $DB_PATH
	CLUSTER_CONF=$DB_PATH/cluster.conf
	REPL_CONF=$DB_PATH/replication.conf
	echo "leveldb" > $DB_PATH/.reindexer.storage

	( echo "app_name: rx_node"
		echo "namespaces: []"
		echo "sync_threads: 4"
		echo "enable_compression: true"
		echo "updates_timeout_sec: 15"
		echo "retry_sync_interval_msec: 5000"
		echo "nodes:"
		node=0
		while [[ $node -lt $NODES_NUMBER ]]
			do
				RPCADDR=$((6534+$node))
				echo "  -" 
				echo "    dsn: cproto://127.0.0.1:$RPCADDR/$CLUSTER_DB_NAME"
				echo "    server_id: $node"
				node=$((node+1))
			done 
	) > $CLUSTER_CONF
	
	
	(echo "cluster_id: 2"
		echo "server_id: $node"
	) > $REPL_CONF
	node=$((node+1))
done

# Run servers
node=0
while [[ $node -lt $NODES_NUMBER ]]
do
	HTTPADDR=$((9088+$node))
	RPCADDR=$((6534+$node))
	DB_PATH="/tmp/rx_gocluster/reindex_test$(($node+1))"
	DB_LOG_CORE="$LOG_BASE_DIR/corelog_$node.txt"
	DB_LOG_RPC="$LOG_BASE_DIR/rpclog_$node.txt"
	DB_LOG_HTTP="$LOG_BASE_DIR/httplog_$node.txt"
	DB_LOG_SERVER="$LOG_BASE_DIR/serverlog_$node.txt"

	build/cpp_src/cmd/reindexer_server/reindexer_server --db=$DB_PATH --loglevel=trace --serverlog="" --corelog=$DB_LOG_CORE --httplog=$DB_LOG_HTTP --rpclog=$DB_LOG_RPC -p $HTTPADDR -r $RPCADDR --enable-cluster &
	SERVER_PID[$node]=$!
	node=$((node+1))
done

# Run tests
DSN=cproto://127.0.0.1:6534/$CLUSTER_DB_NAME
CLUSTER=""
node=1
while [[ $node -lt $NODES_NUMBER ]]
do
	RPCADDR=$((6534+$node))
	NODEADDR="-cluster cproto://127.0.0.1:$RPCADDR/$CLUSTER_DB_NAME "
	CLUSTER+=$NODEADDR
	node=$((node+1))
done

gotestsum --junitfile server_replication_tests.xml ./test -bench . -benchmem -benchtime 100ms -dsn $DSN $CLUSTER -seedcount 50000 -seedcpu 8

# Kill servers
node=0
while [[ $node -lt $NODES_NUMBER ]]
do
	kill ${SERVER_PID[$node]}
	wait ${SERVER_PID[$node]}
	node=$((node+1))
done

