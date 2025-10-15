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

	diff=$(echo "${ns_list_actual[@]}" "${ns_list_expected[@]}" | tr ' ' '\n' | sort | uniq -u) # compare in any order
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


RX_SERVER_CURRENT_VERSION_RPM="$(basename build/reindexer-*server*.rpm)"
VERSION_FROM_RPM=$(echo "$RX_SERVER_CURRENT_VERSION_RPM" | grep -o '.*server-..')
VERSION=$(echo ${VERSION_FROM_RPM: -2:1})   # one-digit version

echo "## choose latest release rpm file"

namespaces_list_expected=$'external_billings\nb2b_device_groups\nmedia_items\nsms_gateways_dict\ncountries_dict\nmy_collection_types_dict\nforbidden_app_versions\npayment_methods_dict\ncontent_filters\nservices\nrecom_epg_vod_similar\ncertificates\nrecom_epg_live_default\nperson_roles\ndevice_type_notifications\nmedia_purchases\nvod_genres\nb2b_clients\ncanvas_images\nusage_models\nepg_genres\nclusters\ncollections\nasset_qualities_old_sdp_link\n#config\npersons\n#queriesperfstats\npromo_categories\nfeature_toggles\nrecom_epg_mixed_default\nasset_video_servers\nfeature_toggle_configs\nrecom_epg_mixed_personal\noffers\n#namespaces\nbonus_programs_dict\ncontent_assets\nb2b_media_views\ncontroller_types_dict\npromo_events\nchannels_themes\nage_limit_dict\npromo_partners\nrecom_epg_live_personal\nservice_tabs\nasset_qualities_rules\nservice_purchases\ncontent_views\nb2b_playlists\ndevices\ncategories\ndo_once_flags\nservice_types_dict\npurchase_groups\nsegments\n#memstats\nsessions\naccounts\nrecorded_programs\nmessages\nbonus_programs\ncurrency_dict\nforbidden_applications\nrecom_epg_archive_personal\ndo_once_locks\nsorts_vod\nrecom_cold_start_matrix\nfeature_flags\nassistants_dict\nsplash_screens\napplications\nepg\nab_tests\nrecom_playlist_personal\nrecom_media_items_similars\nfaq_section\nrecom_media_items_personal\ntimezones\nbonus_abonent_types\nbank_cards\nmedia_position_types_dict\nradio_channels\nb2b_block_items\nproviders\nkaraoke_items\nprofiles\nepg_dv\n#activitystats\nfaq\nblock_screen_templates\nrecom_cold_start_genres\nfavorites\napplication_versions\nbonus_prices\nfirmware_versions\nsubscription_requests\nmedia_ratings\nprofile_icons\nprofile_type_icons_dict\ntext_templates\nrecom_epg_archive_default\ncontent_filter_themes\nlocations\n#clientsstats\npublication_statuses_dict\nad_pixels\n#perfstats\nkaraoke_genres\nchannel_previews\nrecom_media_items_recent_top\nchannels\ndrm_providers_dict\nrecom_media_items_default\nandroid_api_versions_dict\npayment_methods_rules\nsdp_locations\nlanguages_dict\napplication_categories_dict\nrecom_ab_test\ndevice_types_dict\nscreensavers\nvod_discounts\nmedia_positions\npurchases_history\ncpu_archs_dict\nreminders\ndevelopers\ndevice_platform_dict'

if [[ $VERSION == 3 || $VERSION == 4 || $VERSION == 5 ]]; then
	LATEST_RELEASE=$(python3 cpp_src/cmd/reindexer_server/test/get_last_rx_version.py -v $VERSION)
	if [[ $VERSION != 3  ]]; then
		# namespace #replicationstats was added in v4
		namespaces_list_expected=$'#replicationstats\n'"$namespaces_list_expected"
	fi
else
	echo "Unknown version"
	exit 1
fi

echo "## downloading latest release rpm file: $LATEST_RELEASE"
curl "http://repo.itv.restr.im/itv-api-ng/7/x86_64/$LATEST_RELEASE" --output $LATEST_RELEASE;
echo "## downloading database dump"
curl "https://github.com/restream/reindexer_testdata/-/raw/main/dump_fa_demo_v2.zip" --output dump_fa_demo_v2.zip;
unzip -o dump_fa_demo_v2.zip # unzips into frontapi_demo_v2.rxdump;
rm -f dump_fa_demo_v2.zip

ADDRESS="cproto://127.0.0.1:6534/"
DB_NAME="test"

memstats_expected=$'[
{"name":"ab_tests","replication":{"data_hash":744846522577,"data_count":29}},
{"name":"accounts","replication":{"data_hash":152123107609,"data_count":49}},
{"name":"ad_pixels","replication":{"data_hash":7783326768,"data_count":5}},
{"name":"age_limit_dict","replication":{"data_hash":45769690836,"data_count":5}},
{"name":"android_api_versions_dict","replication":{"data_hash":0,"data_count":0}},
{"name":"application_categories_dict","replication":{"data_hash":0,"data_count":0}},
{"name":"application_versions","replication":{"data_hash":0,"data_count":0}},
{"name":"applications","replication":{"data_hash":0,"data_count":0}},
{"name":"asset_qualities_old_sdp_link","replication":{"data_hash":27099245268,"data_count":12}},
{"name":"asset_qualities_rules","replication":{"data_hash":39129915716,"data_count":2}},
{"name":"asset_video_servers","replication":{"data_hash":14259132562,"data_count":98}},
{"name":"assistants_dict","replication":{"data_hash":17189164228,"data_count":11}},
{"name":"b2b_block_items","replication":{"data_hash":3561276292,"data_count":110}},
{"name":"b2b_clients","replication":{"data_hash":4876483092,"data_count":21361}},
{"name":"b2b_device_groups","replication":{"data_hash":9231453801,"data_count":22}},
{"name":"b2b_media_views","replication":{"data_hash":-246369049012,"data_count":15}},
{"name":"b2b_playlists","replication":{"data_hash":2846110980806,"data_count":39}},
{"name":"bank_cards","replication":{"data_hash":0,"data_count":0}},
{"name":"block_screen_templates","replication":{"data_hash":6781131017,"data_count":5}},
{"name":"bonus_abonent_types","replication":{"data_hash":11771370568,"data_count":10}},
{"name":"bonus_prices","replication":{"data_hash":273808884,"data_count":414}},
{"name":"bonus_programs","replication":{"data_hash":0,"data_count":0}},
{"name":"bonus_programs_dict","replication":{"data_hash":116218778663,"data_count":2}},
{"name":"canvas_images","replication":{"data_hash":6440144365,"data_count":40}},
{"name":"categories","replication":{"data_hash":14548251660,"data_count":7}},
{"name":"certificates","replication":{"data_hash":0,"data_count":0}},
{"name":"channel_previews","replication":{"data_hash":0,"data_count":0}},
{"name":"channels","replication":{"data_hash":91572300114425994,"data_count":6208}},
{"name":"channels_themes","replication":{"data_hash":4813556850,"data_count":12}},
{"name":"clusters","replication":{"data_hash":7381671417,"data_count":9}},
{"name":"collections","replication":{"data_hash":161043026088,"data_count":36}},
{"name":"content_assets","replication":{"data_hash":5016235791,"data_count":907341}},
{"name":"content_filter_themes","replication":{"data_hash":526585279,"data_count":9}},
{"name":"content_filters","replication":{"data_hash":27685495954,"data_count":12}},
{"name":"content_views","replication":{"data_hash":0,"data_count":0}},
{"name":"controller_types_dict","replication":{"data_hash":0,"data_count":0}},
{"name":"countries_dict","replication":{"data_hash":16686622555,"data_count":41}},
{"name":"cpu_archs_dict","replication":{"data_hash":0,"data_count":0}},
{"name":"currency_dict","replication":{"data_hash":12319598945,"data_count":3}},
{"name":"developers","replication":{"data_hash":0,"data_count":0}},
{"name":"device_platform_dict","replication":{"data_hash":787247369,"data_count":9}},
{"name":"device_type_notifications","replication":{"data_hash":23342687652,"data_count":11}},
{"name":"device_types_dict","replication":{"data_hash":20949363827,"data_count":1363}},
{"name":"devices","replication":{"data_hash":241895657301,"data_count":479}},
{"name":"do_once_flags","replication":{"data_hash":7078599473,"data_count":98}},
{"name":"do_once_locks","replication":{"data_hash":0,"data_count":0}},
{"name":"drm_providers_dict","replication":{"data_hash":48387685123,"data_count":2}},
{"name":"epg","replication":{"data_hash":0,"data_count":0}},
{"name":"epg_dv","replication":{"data_hash":480692005409992,"data_count":284594}},
{"name":"epg_genres","replication":{"data_hash":3963186654,"data_count":16}},
{"name":"external_billings","replication":{"data_hash":10714865663,"data_count":4}},
{"name":"faq","replication":{"data_hash":26466156010,"data_count":15}},
{"name":"faq_section","replication":{"data_hash":9565379583,"data_count":9}},
{"name":"favorites","replication":{"data_hash":76056185,"data_count":2}},
{"name":"feature_flags","replication":{"data_hash":24316224077,"data_count":47}},
{"name":"feature_toggle_configs","replication":{"data_hash":5831991788,"data_count":8}},
{"name":"feature_toggles","replication":{"data_hash":30220867193,"data_count":18}},
{"name":"firmware_versions","replication":{"data_hash":0,"data_count":0}},
{"name":"forbidden_app_versions","replication":{"data_hash":2098857276,"data_count":3}},
{"name":"forbidden_applications","replication":{"data_hash":0,"data_count":0}},
{"name":"karaoke_genres","replication":{"data_hash":14482621407,"data_count":48}},
{"name":"karaoke_items","replication":{"data_hash":657294710820054,"data_count":8106}},
{"name":"languages_dict","replication":{"data_hash":2248417514,"data_count":4}},
{"name":"locations","replication":{"data_hash":297874230727,"data_count":602}},
{"name":"media_items","replication":{"data_hash":-450383571294080788,"data_count":80127}},
{"name":"media_position_types_dict","replication":{"data_hash":33756054814,"data_count":9}},
{"name":"media_positions","replication":{"data_hash":1661143315621794706,"data_count":15}},
{"name":"media_purchases","replication":{"data_hash":2245808100703,"data_count":830}},
{"name":"media_ratings","replication":{"data_hash":0,"data_count":0}},
{"name":"messages","replication":{"data_hash":0,"data_count":0}},
{"name":"my_collection_types_dict","replication":{"data_hash":20876041916,"data_count":20}},
{"name":"offers","replication":{"data_hash":55746496019,"data_count":1789}},
{"name":"payment_methods_dict","replication":{"data_hash":92642189,"data_count":6}},
{"name":"payment_methods_rules","replication":{"data_hash":103017872200,"data_count":26}},
{"name":"person_roles","replication":{"data_hash":7533647485,"data_count":15}},
{"name":"persons","replication":{"data_hash":23302802393,"data_count":1595601}},
{"name":"profile_icons","replication":{"data_hash":14182092622,"data_count":3}},
{"name":"profile_type_icons_dict","replication":{"data_hash":5697440991,"data_count":24}},
{"name":"profiles","replication":{"data_hash":777225694,"data_count":514}},
{"name":"promo_categories","replication":{"data_hash":5274879239,"data_count":2}},
{"name":"promo_events","replication":{"data_hash":43294888557,"data_count":7}},
{"name":"promo_partners","replication":{"data_hash":2765612704,"data_count":4}},
{"name":"providers","replication":{"data_hash":3725705326,"data_count":7}},
{"name":"publication_statuses_dict","replication":{"data_hash":0,"data_count":0}},
{"name":"purchase_groups","replication":{"data_hash":110345937781,"data_count":8}},
{"name":"purchases_history","replication":{"data_hash":100744672799,"data_count":647}},
{"name":"radio_channels","replication":{"data_hash":65393303340,"data_count":28}},
{"name":"recom_ab_test","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_cold_start_genres","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_cold_start_matrix","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_epg_archive_default","replication":{"data_hash":-463813655296,"data_count":5450}},
{"name":"recom_epg_archive_personal","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_epg_live_default","replication":{"data_hash":-79931449888,"data_count":10422}},
{"name":"recom_epg_live_personal","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_epg_mixed_default","replication":{"data_hash":-362362134816,"data_count":5674}},
{"name":"recom_epg_mixed_personal","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_epg_vod_similar","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_media_items_default","replication":{"data_hash":10024715049,"data_count":3}},
{"name":"recom_media_items_personal","replication":{"data_hash":0,"data_count":0}},
{"name":"recom_media_items_recent_top","replication":{"data_hash":32201979408,"data_count":48}},
{"name":"recom_media_items_similars","replication":{"data_hash":86716527479,"data_count":187425}},
{"name":"recom_playlist_personal","replication":{"data_hash":0,"data_count":0}},
{"name":"recorded_programs","replication":{"data_hash":0,"data_count":0}},
{"name":"reminders","replication":{"data_hash":67916193299784752,"data_count":286}},
{"name":"screensavers","replication":{"data_hash":8129483789,"data_count":4}},
{"name":"sdp_locations","replication":{"data_hash":31216389953,"data_count":89}},
{"name":"segments","replication":{"data_hash":8314874171680846643,"data_count":16}},
{"name":"service_purchases","replication":{"data_hash":932208173603,"data_count":445631}},
{"name":"service_tabs","replication":{"data_hash":15820526444,"data_count":4}},
{"name":"service_types_dict","replication":{"data_hash":5281595988,"data_count":3}},
{"name":"services","replication":{"data_hash":-2082975130960112539,"data_count":9719}},
{"name":"sessions","replication":{"data_hash":2416428933713443,"data_count":48}},
{"name":"sms_gateways_dict","replication":{"data_hash":65758098581,"data_count":7}},
{"name":"sorts_vod","replication":{"data_hash":86355971293,"data_count":10}},
{"name":"splash_screens","replication":{"data_hash":25606557487,"data_count":12}},
{"name":"subscription_requests","replication":{"data_hash":102959065106,"data_count":2227}},
{"name":"text_templates","replication":{"data_hash":2635320219,"data_count":73}},
{"name":"timezones","replication":{"data_hash":17930648862,"data_count":12}},
{"name":"usage_models","replication":{"data_hash":7931483018,"data_count":5}},
{"name":"vod_discounts","replication":{"data_hash":43644486123,"data_count":5}},
{"name":"vod_genres","replication":{"data_hash":14110457746,"data_count":62}}
]
Returned 121 rows'

echo "##### Forward compatibility test #####"

DB_PATH="/tmp/rx_db"

echo "Database: "$DB_PATH
rm -rf "$DB_PATH"

echo "## installing latest release: $LATEST_RELEASE"
yum install -y $LATEST_RELEASE > /dev/null;
# run RX server with disabled logging
reindexer_server -l warning --httplog=none --rpclog=none --db $DB_PATH &
server_pid=$!
sleep 2;

reindexer_tool --dsn $ADDRESS$DB_NAME -f frontapi_demo_v2.rxdump --createdb --threads 4;
sleep 1;

namespaces_1=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command '\namespaces list');
echo $namespaces_1;
CompareNamespacesLists "${namespaces_1[@]}" "${namespaces_list_expected[@]}" $server_pid;

python3 cpp_src/cmd/reindexer_server/test/compare_memstats.py --expected "$memstats_expected" --addr $ADDRESS --db $DB_NAME || { KillAndRemoveServer $server_pid; exit 1; }

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

python3 cpp_src/cmd/reindexer_server/test/compare_memstats.py --expected "$memstats_expected" --addr $ADDRESS --db $DB_NAME || { KillAndRemoveServer $server_pid; exit 1; }

KillAndRemoveServer $server_pid;
rm -rf $DB_PATH;
sleep 1;

echo "##### Backward compatibility test #####"

echo "## installing current version: $RX_SERVER_CURRENT_VERSION_RPM"
yum install -y build/*.rpm > /dev/null;
reindexer_server -l warning --httplog=none --rpclog=none --db $DB_PATH &
server_pid=$!
sleep 2;

reindexer_tool --dsn $ADDRESS$DB_NAME -f frontapi_demo_v2.rxdump --createdb --threads 4;
sleep 1;

namespaces_3=$(reindexer_tool --dsn $ADDRESS$DB_NAME --command '\namespaces list');
echo $namespaces_3;
CompareNamespacesLists "${namespaces_3[@]}" "${namespaces_list_expected[@]}" $server_pid;

python3 cpp_src/cmd/reindexer_server/test/compare_memstats.py --expected "$memstats_expected" --addr $ADDRESS --db $DB_NAME || { KillAndRemoveServer $server_pid; exit 1; }

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

python3 cpp_src/cmd/reindexer_server/test/compare_memstats.py --expected "$memstats_expected" --addr $ADDRESS --db $DB_NAME || { KillAndRemoveServer $server_pid; exit 1; }

KillAndRemoveServer $server_pid;
rm -rf $DB_PATH;
