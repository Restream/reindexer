#!/bin/bash

set -e

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
source $SCRIPT_DIR/tools.sh

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
BASE_DIR="/tmp/rx_go_cluster"
if [[ -n $REINDEXER_TEST_DB_ROOT ]]; then
	BASE_DIR=$REINDEXER_TEST_DB_ROOT/rx_go_cluster
fi
BASE_RPC=6000
BASE_HTTP=7000
RX_BIN=build/cpp_src/cmd/reindexer_server/reindexer_server
QR_TIMEOUT=10

CreateDirForClusterNodes $BASE_DIR $NODES_NUMBER $CLUSTER_DB_NAME $BASE_RPC
RunServers $NODES_NUMBER $BASE_RPC $BASE_HTTP $BASE_DIR $RX_BIN $QR_TIMEOUT

# Run tests
DSN=cproto://127.0.0.1:$BASE_RPC/$CLUSTER_DB_NAME
CLUSTER=""
node=1
while [[ $node -lt $NODES_NUMBER ]]
do
	RPCADDR=$(($BASE_RPC+$node))
	NODEADDR="-cluster cproto://127.0.0.1:$RPCADDR/$CLUSTER_DB_NAME "
	CLUSTER+=$NODEADDR
	node=$((node+1))
done

go test ./test -bench . -benchmem -benchtime 100ms -dsn $DSN $CLUSTER -seedcount 50000 -seedcpu 8 -timeout 25m -tags tiny_vectors

KillServers
