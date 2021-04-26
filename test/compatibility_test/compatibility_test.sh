#!/bin/bash

set -e
#set -o xtrace

[ -z "$1" ] && echo "Expect reindexer version as an argument" && exit -1

rpm_url="http://repo.itv.restr.im/itv-api-ng/7/x86_64/reindexer-server-$1-1.x86_64.rpm"
master_db_path="/tmp/reindex_test_master"
slave_db_path="/tmp/reindex_test_slave"
master_dsn="cproto://127.0.0.1:6534"
slave_dsn="cproto://127.0.0.1:6535"
master_dump="master_dump"
slave_dump="slave_dump"
ns_name="items"
db_name="testdb"
script_dir="$( cd "$(dirname "$0")" ; pwd -P )"

clear_artifacts(){
	rm -rf "${master_db_path}"
	rm -rf "${slave_db_path}"
	rm -f "${master_dump}"
	rm -f "${slave_dump}"
}

init_storages(){
	clear_artifacts
	mkdir -p ${slave_db_path}/${db_name}
	echo -n "leveldb" > ${slave_db_path}/${db_name}/.reindexer.storage
	cp ${script_dir}/replication_slave.conf ${slave_db_path}/${db_name}/replication.conf
	mkdir -p ${master_db_path}/${db_name}
	echo -n "leveldb" > ${master_db_path}/${db_name}/.reindexer.storage
	cp ${script_dir}/replication_master.conf ${master_db_path}/${db_name}/replication.conf
}

test_outdated_instance() {
	master_cmd=$1
	slave_cmd=$2
	echo "====Master: ${master_cmd}"
	echo "====Slave: ${slave_cmd}"
	init_storages
	${master_cmd} --db "${master_db_path}" -l0 --serverlog=\"\" --corelog=\"\" --httplog=\"\" --rpclog=\"\" &
	master_pid=$!
	sleep 1
	go run ${script_dir}/filler.go --dsn "${master_dsn}/${db_name}" --offset 0
	echo "====Force sync"
	${slave_cmd} --db "${slave_db_path}" -p 9089 -r 6535 -l0 --serverlog=\"\" --corelog=\"\" --httplog=\"\" --rpclog=\"\" &
	slave_pid=$!
	sleep 5
	kill $slave_pid
    wait $slave_pid
	go run ${script_dir}/filler.go --dsn "${master_dsn}/${db_name}" --offset 100
	echo "====Sync by WAL"
	${slave_cmd} --db "${slave_db_path}" -p 9089 -r 6535 -l0 --serverlog=\"\" --corelog=\"\" --httplog=\"\" --rpclog=\"\" &
	slave_pid=$!
	sleep 5
	echo "====Online sync"
	go run ${script_dir}/filler.go --dsn "${master_dsn}/${db_name}" --offset 200
	sleep 3
	build/cpp_src/cmd/reindexer_tool/reindexer_tool --dsn "${master_dsn}/${db_name}" --command "\dump ${ns_name}" --output "${master_dump}"
	build/cpp_src/cmd/reindexer_tool/reindexer_tool --dsn "${slave_dsn}/${db_name}" --command "\dump ${ns_name}" --output "${slave_dump}"
	kill $slave_pid
    wait $slave_pid
    kill $master_pid
    wait $master_pid
    sed -i -E "s/(\\NAMESPACES ADD.*)(\"schema\":\"\{.*\}\")/\1\"schema\":\"\{\}\"/" "${master_dump}"
    ${script_dir}/compare_dumps.sh "${master_dump}" "${slave_dump}"
}

echo "====Installing reindexer package===="
echo "====URL: ${rpm_url}"
yum install -y ${rpm_url} > /dev/null || true
echo "====Checking outdated slave===="
test_outdated_instance "build/cpp_src/cmd/reindexer_server/reindexer_server" "reindexer_server"
echo "====Checking outdated master===="
test_outdated_instance "reindexer_server" "build/cpp_src/cmd/reindexer_server/reindexer_server"
clear_artifacts

