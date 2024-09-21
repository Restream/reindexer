#!/bin/bash
# Task: https://github.com/restream/reindexer/-/issues/1188
set -e

function KillAndRemoveServer {
	local pid=$1
	kill $pid
	wait $pid
	yum remove -y 'reindexer*' > /dev/null
}

function WaitForDB {
	# wait until DB is loaded
	set +e # disable "exit on error" so the script won't stop when DB's not loaded yet
	is_connected=$(reindexer_tool --dsn $ADDRESS --command '\databases list');
	while [[ $is_connected != "test" ]]
		do
			sleep 2
			is_connected=$(reindexer_tool --dsn $ADDRESS --command '\databases list');
		done
	set -e
}

function CompareNamespacesLists {
	local ns_list_actual=$1
	local ns_list_expected=$2
	local pid=$3

	diff=$(echo ${ns_list_actual[@]} ${ns_list_expected[@]} | tr ' ' '\n' | sort | uniq -u) # compare in any order
	if [ "$diff" == "" ]; then
	    echo "## PASS: namespaces list not changed"
	else
	    echo "##### FAIL: namespaces list was changed"
	    echo "expected: $ns_list_expected"
	    echo "actual: $ns_list_actual"
		KillAndRemoveServer $pid;
	    exit 1
    fi
}

function CompareMemstats {
	local actual=$1
	local expected=$2
	local pid=$3
	diff=$(echo ${actual[@]} ${expected[@]} | tr ' ' '\n' | sed 's/\(.*\),$/\1/' | sort | uniq -u) # compare in any order
	if [ "$diff" == "" ]; then
	    echo "## PASS: memstats not changed"
	else
	    echo "##### FAIL: memstats was changed"
	    echo "expected: $expected"
	    echo "actual: $actual"
		KillAndRemoveServer $pid;
	    exit 1
    fi
}


RX_SERVER_CURRENT_VERSION_RPM="$(basename build/reindexer-*server*.rpm)"
VERSION_FROM_RPM=$(echo "$RX_SERVER_CURRENT_VERSION_RPM" | grep -o '.*server-..')
VERSION=$(echo ${VERSION_FROM_RPM: -2:1})   # one-digit version

echo "## choose latest release rpm file"
if [ $VERSION == 3 ]; then
  LATEST_RELEASE=$(python3 cpp_src/cmd/reindexer_server/test/get_last_rx_version.py -v 3)
  namespaces_list_expected=$'purchase_options_ext_dict\nchild_account_recommendations\n#config\n#activitystats\nradio_channels\ncollections\n#namespaces\nwp_imports_tasks\nepg_genres\nrecom_media_items_personal\nrecom_epg_archive_default\n#perfstats\nrecom_epg_live_default\nmedia_view_templates\nasset_video_servers\nwp_tasks_schedule\nadmin_roles\n#clientsstats\nrecom_epg_archive_personal\nrecom_media_items_similars\nmenu_items\naccount_recommendations\nkaraoke_items\nmedia_items\nbanners\n#queriesperfstats\nrecom_media_items_default\nrecom_epg_live_personal\nservices\n#memstats\nchannels\nmedia_item_recommendations\nwp_tasks_tasks\nepg'
elif [ $VERSION == 4 ]; then
  LATEST_RELEASE=$(python3 cpp_src/cmd/reindexer_server/test/get_last_rx_version.py -v 4)
  # replicationstats ns added for v4
  namespaces_list_expected=$'purchase_options_ext_dict\nchild_account_recommendations\n#config\n#activitystats\n#replicationstats\nradio_channels\ncollections\n#namespaces\nwp_imports_tasks\nepg_genres\nrecom_media_items_personal\nrecom_epg_archive_default\n#perfstats\nrecom_epg_live_default\nmedia_view_templates\nasset_video_servers\nwp_tasks_schedule\nadmin_roles\n#clientsstats\nrecom_epg_archive_personal\nrecom_media_items_similars\nmenu_items\naccount_recommendations\nkaraoke_items\nmedia_items\nbanners\n#queriesperfstats\nrecom_media_items_default\nrecom_epg_live_personal\nservices\n#memstats\nchannels\nmedia_item_recommendations\nwp_tasks_tasks\nepg'
else
  echo "Unknown version"
  exit 1
fi

echo "## downloading latest release rpm file: $LATEST_RELEASE"
curl "http://repo.itv.restr.im/itv-api-ng/7/x86_64/$LATEST_RELEASE" --output $LATEST_RELEASE;
echo "## downloading example DB"
curl "https://github.com/restream/reindexer_testdata/-/raw/main/dump_demo.zip" --output dump_demo.zip;
unzip -o dump_demo.zip # unzips into demo_test.rxdump;

ADDRESS="cproto://127.0.0.1:6534/"
DB_NAME="test"

memstats_expected=$'[
{"name":"account_recommendations","replication":{"data_hash":6833710705,"data_count":1}},
{"name":"admin_roles","replication":{"data_hash":1896088071,"data_count":2}},
{"name":"asset_video_servers","replication":{"data_hash":7404222244,"data_count":97}},
{"name":"banners","replication":{"data_hash":0,"data_count":0}},
{"name":"channels","replication":{"data_hash":457292509431319,"data_count":3941}},
{"name":"child_account_recommendations","replication":{"data_hash":6252344969,"data_count":1}},
{"name":"collections","replication":{"data_hash":0,"data_count":0}},
{"name":"epg","replication":{"data_hash":-7049751653258,"data_count":1623116}},
{"name":"epg_genres","replication":{"data_hash":8373644068,"data_count":1315}},
{"name":"karaoke_items","replication":{"data_hash":5858155773472,"data_count":4500}},
{"name":"media_item_recommendations","replication":{"data_hash":-6520334670,"data_count":35886}},
{"name":"media_items","replication":{"data_hash":-1824301168479972392,"data_count":65448}},
{"name":"media_view_templates","replication":{"data_hash":0,"data_count":0}},
{"name":"menu_items","replication":{"data_hash":0,"data_count":0}},
{"name":"purchase_options_ext_dict","replication":{"data_hash":24651210926,"data_count":3}},
{"name":"radio_channels","replication":{"data_hash":37734732881,"data_count":28}},
{"name":"recom_epg_archive_default","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_epg_archive_personal","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_epg_live_default","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_epg_live_personal","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_media_items_default","replication":{"data_hash":8288213744,"data_count":3}},
{"name":"recom_media_items_personal","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_media_items_similars","replication":{"data_hash":-672103903,"data_count":33538}},
{"name":"services","replication":{"data_hash":0,"data_count":0}},
{"name":"wp_imports_tasks","replication":{"data_hash":777859741066,"data_count":1145}},
{"name":"wp_tasks_schedule","replication":{"data_hash":12595790956,"data_count":4}},
{"name":"wp_tasks_tasks","replication":{"data_hash":28692716680,"data_count":281}}
]
Returned 27 rows'

echo "##### Forward compatibility test #####"

DB_PATH=$(pwd)"/rx_db"

echo "Database: "$DB_PATH

echo "## installing latest release: $LATEST_RELEASE"
yum install -y $LATEST_RELEASE > /dev/null;
# run RX server with disabled logging
reindexer_server -l warning --httplog=none --rpclog=none --db $DB_PATH &
server_pid=$!
sleep 2;

reindexer_tool --dsn $ADDRESS$DB_NAME -f demo_test.rxdump --createdb;
sleep 1;

namespaces_1=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command '\namespaces list');
echo $namespaces_1;
CompareNamespacesLists "${namespaces_1[@]}" "${namespaces_list_expected[@]}" $server_pid;

memstats_1=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command 'select name, replication.data_hash, replication.data_count from #memstats order by name');
CompareMemstats "${memstats_1[@]}" "${memstats_expected[@]}" $server_pid;

KillAndRemoveServer $server_pid;

echo "## installing current version: $RX_SERVER_CURRENT_VERSION_RPM"
yum install -y build/*.rpm > /dev/null;
reindexer_server -l0  --corelog=none --httplog=none --rpclog=none --db $DB_PATH &
server_pid=$!
sleep 2;

WaitForDB

namespaces_2=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command '\namespaces list');
echo $namespaces_2;
CompareNamespacesLists "${namespaces_2[@]}" "${namespaces_1[@]}" $server_pid;


memstats_2=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command 'select name, replication.data_hash, replication.data_count from #memstats order by name');
CompareMemstats "${memstats_2[@]}" "${memstats_1[@]}" $server_pid;

KillAndRemoveServer $server_pid;
rm -rf $DB_PATH;
sleep 1;

echo "##### Backward compatibility test #####"

echo "## installing current version: $RX_SERVER_CURRENT_VERSION_RPM"
yum install -y build/*.rpm > /dev/null;
reindexer_server -l warning --httplog=none --rpclog=none --db $DB_PATH &
server_pid=$!
sleep 2;

reindexer_tool --dsn $ADDRESS$DB_NAME -f demo_test.rxdump --createdb;
sleep 1;

namespaces_3=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command '\namespaces list');
echo $namespaces_3;
CompareNamespacesLists "${namespaces_3[@]}" "${namespaces_list_expected[@]}" $server_pid;


memstats_3=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command 'select name, replication.data_hash, replication.data_count from #memstats order by name');
CompareMemstats "${memstats_3[@]}" "${memstats_expected[@]}" $server_pid;

KillAndRemoveServer $server_pid;

echo "## installing latest release: $LATEST_RELEASE"
yum install -y $LATEST_RELEASE > /dev/null;
reindexer_server -l warning --httplog=none --rpclog=none --db $DB_PATH &
server_pid=$!
sleep 2;

WaitForDB

namespaces_4=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command '\namespaces list');
echo $namespaces_4;
CompareNamespacesLists "${namespaces_4[@]}" "${namespaces_3[@]}" $server_pid;

memstats_4=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command 'select name, replication.data_hash, replication.data_count from #memstats order by name');
CompareMemstats "${memstats_4[@]}" "${memstats_3[@]}" $server_pid;

KillAndRemoveServer $server_pid;
rm -rf $DB_PATH;
