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

CLUSTER_DB_NAME=items
BASE_DIR="/tmp/rx_java_cluster"
if [[ -n $REINDEXER_TEST_DB_ROOT ]]; then
	BASE_DIR=$REINDEXER_TEST_DB_ROOT/rx_java_cluster
fi
BASE_RPC=6534
BASE_HTTP=9088
RX_BIN=reindexer_server

CreateDirForClusterNodes $BASE_DIR $NODES_NUMBER $CLUSTER_DB_NAME $BASE_RPC
RunServers $NODES_NUMBER $BASE_RPC $BASE_HTTP $BASE_DIR $RX_BIN

# Set leader
reindexer_tool --dsn 'cproto://127.0.0.1:6534/items' --command '\upsert #config {"type":"action","action":{"command":"set_leader_node", "server_id": 2}}'
echo "Leader was changed"
# Run java tests on follower
DSN_FOLLOWER=cproto://127.0.0.1:$BASE_RPC/$CLUSTER_DB_NAME
cd /builds/itv-backend/reindexer/reindexer-java
mvn test -Dtests=cproto -DCprotoDsns=$DSN_FOLLOWER

KillServers
