#!/bin/sh

RX_ARGS="--db $RX_DATABASE --httpaddr 0:9088 --rpcaddr 0:6534 --webroot /usr/local/share/reindexer/web --corelog $RX_CORELOG --serverlog $RX_SERVERLOG --httplog $RX_HTTPLOG --rpclog $RX_RPCLOG --loglevel $RX_LOGLEVEL --grpc"

mkdir -p $RX_DATABASE

if [ -n "$RX_PPROF" ]; then
    RX_ARGS="$RX_ARGS --pprof --allocs"
    export TCMALLOC_SAMPLE_PARAMETER=512000
    export MALLOC_CONF=prof:true
fi

if [ -n "$RX_SECURITY" ]; then
    RX_ARGS="$RX_ARGS --security"
fi

if [ -n "$RX_PROMETHEUS" ]; then
    RX_ARGS="$RX_ARGS --prometheus"
fi

if [ -z "$@" ]; then
   reindexer_server $RX_ARGS
else 
   exec "$@"
fi
