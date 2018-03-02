#!/bin/bash

set -e

databases='reindex sqlite tarantool redis mysql mongo clickhouse elastic sphinx arango rethink'
benches='byid 1cond 2cond text text_prefix update'

function run_benchmark  {
    db=$1
    bench=$2
    result=''
    for try in `seq 10`; do
       ret=`wrk -d2s -t2 -c50 http://127.0.0.1:8081/$bench/$db | grep 'Requests\/sec' | sed 's/Requests\/sec:\(.*\)/\1/'`
       result="$result $ret"
       sleep 1
    done
    best=`echo $result | sed s'/ /\n/g' | sort -n -r | head -n1`
    echo "$db $bench -> $best ($result)"
    sleep 5
}


for db in $databases; do
    for bench in $benches; do
        case "$db $bench" in
            "redis 2cond"|"redis text"*|"sphinx byid"|"sphinx 1cond"|"sphinx 2cond"|"clickhouse text"*|"rethink text"*)
            ;;
            *)
            run_benchmark $db $bench
        esac
    done
done

