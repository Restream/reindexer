#!/bin/sh

CLUSTER_DB_NAME=cluster_db

RX_ARGS="--httpaddr 0:9088 --rpcaddr 0:6534 --enable-cluster --webroot /usr/local/share/reindexer/web --corelog $RX_CORELOG --serverlog $RX_SERVERLOG --httplog $RX_HTTPLOG --rpclog $RX_RPCLOG --loglevel $RX_LOGLEVEL"

if [ -n "$RX_DATABASE" ]; then
   mkdir -p $RX_DATABASE/$CLUSTER_DB_NAME
fi

CLUSTER_CONF=$RX_DATABASE/$CLUSTER_DB_NAME/cluster.conf
REPL_CONF=$RX_DATABASE/$CLUSTER_DB_NAME/replication.conf

echo "leveldb" > $RX_DATABASE/$CLUSTER_DB_NAME/.reindexer.storage
>$CLUSTER_CONF
echo "app_name: rx_node" >> $CLUSTER_CONF
echo "namespaces: []" >> $CLUSTER_CONF
echo "sync_threads: 4" >> $CLUSTER_CONF
echo "enable_compression: true" >> $CLUSTER_CONF
echo "online_updates_timeout_sec: 20" >> $CLUSTER_CONF
echo "sync_timeout_sec: 60" >> $CLUSTER_CONF
echo "leader_sync_threads: 8" >> $CLUSTER_CONF
echo "leader_sync_concurrent_snapshots_per_node: 2" >> $CLUSTER_CONF
echo "max_wal_depth_on_force_sync: 1000" >> $CLUSTER_CONF
echo "retry_sync_interval_msec: 5000" >> $CLUSTER_CONF
echo "nodes:" >> $CLUSTER_CONF
node=0
while [ "$node" -lt "$RX_CLUSTER_NODES" ]
do
   echo "  -" >> $CLUSTER_CONF
   echo "    dsn: cproto://node$node:6534/$CLUSTER_DB_NAME" >> $CLUSTER_CONF
   echo "    server_id: $node" >> $CLUSTER_CONF
   node=`expr $node + 1`
done

>$REPL_CONF
echo "cluster_id: 2" >> $REPL_CONF
echo "server_id: $RX_SERVER_ID" >> $REPL_CONF
cat $REPL_CONF

if [ -n "$RX_PPROF" ]; then
    RX_ARGS="$RX_ARGS --pprof --allocs"
    export TCMALLOC_SAMPLE_PARAMETER=512000
    export MALLOC_CONF=prof:true
fi

if [ -n "$RX_PROMETHEUS" ]; then
    RX_ARGS="$RX_ARGS --prometheus"
fi

if [ -z "$@" ]; then
   reindexer_server --db "$RX_DATABASE" $RX_ARGS
else 
   exec "$@"
fi
