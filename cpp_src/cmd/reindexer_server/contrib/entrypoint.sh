#!/bin/sh

RX_ARGS="--db $RX_DATABASE --httpaddr 0:9088 --rpcaddr 0:6534 --httpsaddr 0:9089 --rpcsaddr 0:6535  --webroot /usr/local/share/reindexer/web --corelog $RX_CORELOG --serverlog $RX_SERVERLOG --httplog $RX_HTTPLOG --rpclog $RX_RPCLOG --loglevel $RX_LOGLEVEL --grpc"

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

if [ -n "$RX_RPC_QR_IDLE_TIMEOUT" ]; then
    RX_ARGS="$RX_ARGS --rpc-qr-idle-timeout $RX_RPC_QR_IDLE_TIMEOUT"
else
    RX_ARGS="$RX_ARGS --rpc-qr-idle-timeout 0"
fi

if [ -n "$RX_DISABLE_NS_LEAK" ]; then
    RX_ARGS="$RX_ARGS --disable-ns-leak"
fi

if [ -n "$RX_MAX_HTTP_REQ" ]; then
    RX_ARGS="$RX_ARGS --max-http-req $RX_MAX_HTTP_REQ"
fi

if [ -n "$RX_HTTP_READ_TIMEOUT" ]; then
    RX_ARGS="$RX_ARGS --http-read-timeout $RX_HTTP_READ_TIMEOUT"
fi

if [ -n "$RX_HTTP_WRITE_TIMEOUT" ]; then
    RX_ARGS="$RX_ARGS --http-write-timeout $RX_HTTP_WRITE_TIMEOUT"
fi

if [ -n "$RX_SSL_CERT" ]; then
    RX_ARGS="$RX_ARGS --ssl-cert $RX_SSL_CERT"
fi

if [ -n "$RX_SSL_KEY" ]; then
    RX_ARGS="$RX_ARGS --ssl-key $RX_SSL_KEY"
fi

if [ -z "$@" ]; then
   reindexer_server $RX_ARGS
else 
   exec "$@"
fi
