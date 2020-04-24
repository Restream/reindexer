#!/bin/bash

set -e
dump=${1}
test_db=${2}
source_tree_path="."
[ "${3}" != "" ] && source_tree_path=${3}
test_mode=${4}
test_dump="/tmp/dbtest.dump.tmp"
test_srv_dump="/tmp/dbtest.dump.tmp"

reindexer_tool=${source_tree_path}/build/cpp_src/cmd/reindexer_tool/reindexer_tool
compare_dumps=${source_tree_path}/test/compatibility_test/compare_dumps.sh

rm -rf ${test_db}
${reindexer_tool} --dsn "builtin://${test_db}" --filename "${dump}"
${reindexer_tool} --dsn "builtin://${test_db}" --command '\dump testns' --output "${test_dump}"
${compare_dumps} "${dump}" "${test_dump}"
rm -f "${test_dump}"
${reindexer_tool} --dsn "builtin://${test_db}" --repair
${reindexer_tool} --dsn "builtin://${test_db}" --command '\dump testns' --output "${test_dump}"
${compare_dumps} "${dump}" "${test_dump}"
rm -f "${test_dump}"
rm -rf ${test_db}
${source_tree_path}/build/cpp_src/cmd/reindexer_server/reindexer_server --db "${test_db}" --rpclog \"\" &
server_pid=$!
sleep 3
succeed=true
${reindexer_tool} --dsn cproto://127.0.0.1:6534/dump_test --filename "${dump}" || succeed=false
[ ${succeed} == false ] || (echo "Error expected" && exit 1)
${reindexer_tool} --dsn cproto://127.0.0.1:6534/dump_test --command '\databases create dump_test'
${reindexer_tool} --dsn cproto://127.0.0.1:6534/dump_test --filename "${dump}"
${reindexer_tool} --dsn cproto://127.0.0.1:6534/dump_test --command '\dump testns' --output "${test_srv_dump}"
${compare_dumps} "${dump}" "${test_srv_dump}"
rm -f "${test_srv_dump}"
kill ${server_pid}
wait ${server_pid}
${source_tree_path}/cpp_src/cmd/reindexer_tool/contrib/manual_repair_test.sh "${test_db}" "${source_tree_path}/build"
if [ "${test_mode}" == "full" ]; then
	${source_tree_path}/cpp_src/cmd/reindexer_tool/contrib/auto_repair_test.sh "${test_db}" "${source_tree_path}/build"
fi

