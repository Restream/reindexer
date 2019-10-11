#!/bin/sh

set -e
test_db="$1"
[ -z "${test_db}" ] && echo "usage $0 <test_db>" && exit 1
script_dir="$( cd "$(dirname "$0")" ; pwd -P )"
archive_path="${script_dir}"/testdb.tar.bz2
rm -rf "${test_db}"
mkdir -p "${test_db}"
tar -xjf ${archive_path} -C "${test_db}"
rm -f "${test_db}/test/items/000007.sst"
> "${test_db}/test/items/.rdx_shutdown"
build/cpp_src/cmd/reindexer_server/reindexer_server --db "${test_db}" &
server_pid=$!
sleep 1
kill ${server_pid} || true
wait ${server_pid}

