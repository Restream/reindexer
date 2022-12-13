#!/bin/bash

# This script creates basic configuration for RAFT cluster with 3 nodes.
# DB name is 'cluster_db' (it will also be created in this script). All the namespaces of this DB are in cluster.
#
#          node0
#         /      \
#        /        \
#     node1------node2
#

set -e

# Include helper functions to run servers and create DB directories
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
source $SCRIPT_DIR/tools.sh

export RX_PRINT_CLUSTER_STARTUP=1

# Default settings
# Nodes in cluster
NODES_NUMBER=3
# DB name
CLUSTER_DB_NAME=cluster_db
# Base storage directory (actual DB's directories will be created as a subdirectorires)
BASE_DIR="/tmp/rx_go_cluster"
# RPC-port for the first node. Actual RPC ports will be calcualted as (BASE_RPC + NODE_NUMBER)
BASE_RPC=6534
# HTTP-port for the first node. Actual HTTP ports will be calcualted as (BASE_HTTP + NODE_NUMBER)
BASE_HTTP=9088
# Path to reindexer_server binary
RX_BIN=reindexer_server
# Default timeout for inactive RPC QueryResults
QR_TIMEOUT=600
# Core log level
LOG_LEVEL=info
# Directory for the cluster logs
LOG_BASE_DIR=logs

# Create DB directories for all the cluster nodes
CreateDirForClusterNodes $BASE_DIR $NODES_NUMBER $CLUSTER_DB_NAME $BASE_RPC
# Run cluster's servers
RunServers $NODES_NUMBER $BASE_RPC $BASE_HTTP $BASE_DIR $RX_BIN $QR_TIMEOUT $LOG_LEVEL $LOG_BASE_DIR
