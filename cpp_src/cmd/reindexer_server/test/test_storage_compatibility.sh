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

namespaces_list_expected=$'#replicationstats\nexternal_billings\nb2b_device_groups\nmedia_items\nsms_gateways_dict\ncountries_dict\nmy_collection_types_dict\nforbidden_app_versions\npayment_methods_dict\ncontent_filters\nservices\nrecom_epg_vod_similar\ncertificates\nrecom_epg_live_default\nperson_roles\ndevice_type_notifications\nmedia_purchases\nvod_genres\nb2b_clients\ncanvas_images\nusage_models\nepg_genres\nclusters\ncollections\nasset_qualities_old_sdp_link\n#config\npersons\n#queriesperfstats\npromo_categories\nfeature_toggles\nrecom_epg_mixed_default\nasset_video_servers\nfeature_toggle_configs\nrecom_epg_mixed_personal\noffers\n#namespaces\nbonus_programs_dict\ncontent_assets\nb2b_media_views\ncontroller_types_dict\npromo_events\nchannels_themes\nage_limit_dict\npromo_partners\nrecom_epg_live_personal\nservice_tabs\nasset_qualities_rules\nservice_purchases\ncontent_views\nb2b_playlists\ndevices\ncategories\ndo_once_flags\nservice_types_dict\npurchase_groups\nsegments\n#memstats\nsessions\naccounts\nrecorded_programs\nmessages\nbonus_programs\ncurrency_dict\nforbidden_applications\nrecom_epg_archive_personal\ndo_once_locks\nsorts_vod\nrecom_cold_start_matrix\nfeature_flags\nassistants_dict\nsplash_screens\napplications\nepg\nab_tests\nrecom_playlist_personal\nrecom_media_items_similars\nfaq_section\nrecom_media_items_personal\ntimezones\nbonus_abonent_types\nbank_cards\nmedia_position_types_dict\nradio_channels\nb2b_block_items\nproviders\nkaraoke_items\nprofiles\nepg_dv\n#activitystats\nfaq\nblock_screen_templates\nrecom_cold_start_genres\nfavorites\napplication_versions\nbonus_prices\nfirmware_versions\nsubscription_requests\nmedia_ratings\nprofile_icons\nprofile_type_icons_dict\ntext_templates\nrecom_epg_archive_default\ncontent_filter_themes\nlocations\n#clientsstats\npublication_statuses_dict\nad_pixels\n#perfstats\nkaraoke_genres\nchannel_previews\nrecom_media_items_recent_top\nchannels\ndrm_providers_dict\nrecom_media_items_default\nandroid_api_versions_dict\npayment_methods_rules\nsdp_locations\nlanguages_dict\napplication_categories_dict\nrecom_ab_test\ndevice_types_dict\nscreensavers\nvod_discounts\nmedia_positions\npurchases_history\ncpu_archs_dict\nreminders\ndevelopers\ndevice_platform_dict'

if [[ $VERSION == 5 ]]; then
	LATEST_RELEASE=$(python3 cpp_src/cmd/reindexer_server/test/get_last_rx_version.py -v $VERSION --version_suffix static_leveldb)
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
{"name":"ab_tests","replication":{"checksum":-3830411751431056077,"data_count":29}},
{"name":"accounts","replication":{"checksum":-575356916325268616,"data_count":49}},
{"name":"ad_pixels","replication":{"checksum":6535824531355205277,"data_count":5}},
{"name":"age_limit_dict","replication":{"checksum":-5660192509494337743,"data_count":5}},
{"name":"android_api_versions_dict","replication":{"checksum":0,"data_count":0}},
{"name":"application_categories_dict","replication":{"checksum":0,"data_count":0}},
{"name":"application_versions","replication":{"checksum":0,"data_count":0}},
{"name":"applications","replication":{"checksum":0,"data_count":0}},
{"name":"asset_qualities_old_sdp_link","replication":{"checksum":7143297234148995542,"data_count":12}},
{"name":"asset_qualities_rules","replication":{"checksum":-8707188233161709776,"data_count":2}},
{"name":"asset_video_servers","replication":{"checksum":1212088492930419232,"data_count":98}},
{"name":"assistants_dict","replication":{"checksum":-2676858403997418545,"data_count":11}},
{"name":"b2b_block_items","replication":{"checksum":926487122340422614,"data_count":110}},
{"name":"b2b_clients","replication":{"checksum":5720349769933108260,"data_count":21361}},
{"name":"b2b_device_groups","replication":{"checksum":-324989969183995566,"data_count":22}},
{"name":"b2b_media_views","replication":{"checksum":9166084378107563702,"data_count":15}},
{"name":"b2b_playlists","replication":{"checksum":-6692553155502724574,"data_count":39}},
{"name":"bank_cards","replication":{"checksum":0,"data_count":0}},
{"name":"block_screen_templates","replication":{"checksum":1256130227713155420,"data_count":5}},
{"name":"bonus_abonent_types","replication":{"checksum":-2766500167651067462,"data_count":10}},
{"name":"bonus_prices","replication":{"checksum":6452939545500083801,"data_count":414}},
{"name":"bonus_programs","replication":{"checksum":0,"data_count":0}},
{"name":"bonus_programs_dict","replication":{"checksum":4077223845500621128,"data_count":2}},
{"name":"canvas_images","replication":{"checksum":-7396999167397482261,"data_count":40}},
{"name":"categories","replication":{"checksum":-1529257690449061672,"data_count":7}},
{"name":"certificates","replication":{"checksum":0,"data_count":0}},
{"name":"channel_previews","replication":{"checksum":0,"data_count":0}},
{"name":"channels","replication":{"checksum":-3959969898815373120,"data_count":6208}},
{"name":"channels_themes","replication":{"checksum":-9134422903198265117,"data_count":12}},
{"name":"clusters","replication":{"checksum":7952986246321051358,"data_count":9}},
{"name":"collections","replication":{"checksum":7061492121401733007,"data_count":36}},
{"name":"content_assets","replication":{"checksum":1356769728266967642,"data_count":907341}},
{"name":"content_filter_themes","replication":{"checksum":-5770845395031801953,"data_count":9}},
{"name":"content_filters","replication":{"checksum":6404860322772246751,"data_count":12}},
{"name":"content_views","replication":{"checksum":0,"data_count":0}},
{"name":"controller_types_dict","replication":{"checksum":0,"data_count":0}},
{"name":"countries_dict","replication":{"checksum":7852281983839453950,"data_count":41}},
{"name":"cpu_archs_dict","replication":{"checksum":0,"data_count":0}},
{"name":"currency_dict","replication":{"checksum":-6245482399042004698,"data_count":3}},
{"name":"developers","replication":{"checksum":0,"data_count":0}},
{"name":"device_platform_dict","replication":{"checksum":-2703787240229494743,"data_count":9}},
{"name":"device_type_notifications","replication":{"checksum":-5563872480390801366,"data_count":11}},
{"name":"device_types_dict","replication":{"checksum":3292796979655692525,"data_count":1363}},
{"name":"devices","replication":{"checksum":2397777980345635925,"data_count":479}},
{"name":"do_once_flags","replication":{"checksum":7143403943759034589,"data_count":98}},
{"name":"do_once_locks","replication":{"checksum":0,"data_count":0}},
{"name":"drm_providers_dict","replication":{"checksum":6099760268591247471,"data_count":2}},
{"name":"epg","replication":{"checksum":0,"data_count":0}},
{"name":"epg_dv","replication":{"checksum":433122306055632685,"data_count":284594}},
{"name":"epg_genres","replication":{"checksum":4701214173308979838,"data_count":16}},
{"name":"external_billings","replication":{"checksum":-761728933392098162,"data_count":4}},
{"name":"faq","replication":{"checksum":-3753948766448874587,"data_count":15}},
{"name":"faq_section","replication":{"checksum":4421773179504749262,"data_count":9}},
{"name":"favorites","replication":{"checksum":5307198615045086499,"data_count":2}},
{"name":"feature_flags","replication":{"checksum":-5909490062751609142,"data_count":47}},
{"name":"feature_toggle_configs","replication":{"checksum":-208587040869427663,"data_count":8}},
{"name":"feature_toggles","replication":{"checksum":581833298429099451,"data_count":18}},
{"name":"firmware_versions","replication":{"checksum":0,"data_count":0}},
{"name":"forbidden_app_versions","replication":{"checksum":-16844562825421138,"data_count":3}},
{"name":"forbidden_applications","replication":{"checksum":0,"data_count":0}},
{"name":"karaoke_genres","replication":{"checksum":1050033595256628215,"data_count":48}},
{"name":"karaoke_items","replication":{"checksum":702525622384049675,"data_count":8106}},
{"name":"languages_dict","replication":{"checksum":3204788871321335018,"data_count":4}},
{"name":"locations","replication":{"checksum":-2489071512407739804,"data_count":602}},
{"name":"media_items","replication":{"checksum":2716606612703544608,"data_count":80127}},
{"name":"media_position_types_dict","replication":{"checksum":2753660400526518843,"data_count":9}},
{"name":"media_positions","replication":{"checksum":-8874189439227465081,"data_count":15}},
{"name":"media_purchases","replication":{"checksum":-2607644750661034426,"data_count":830}},
{"name":"media_ratings","replication":{"checksum":0,"data_count":0}},
{"name":"messages","replication":{"checksum":0,"data_count":0}},
{"name":"my_collection_types_dict","replication":{"checksum":1123345921497309132,"data_count":20}},
{"name":"offers","replication":{"checksum":8455742675553264069,"data_count":1789}},
{"name":"payment_methods_dict","replication":{"checksum":4027474431937552338,"data_count":6}},
{"name":"payment_methods_rules","replication":{"checksum":-5174320024155203465,"data_count":26}},
{"name":"person_roles","replication":{"checksum":8103344702048168251,"data_count":15}},
{"name":"persons","replication":{"checksum":-5975193105021313470,"data_count":1595601}},
{"name":"profile_icons","replication":{"checksum":-1595749309305992602,"data_count":3}},
{"name":"profile_type_icons_dict","replication":{"checksum":-1673712882689295545,"data_count":24}},
{"name":"profiles","replication":{"checksum":1385543596401705672,"data_count":514}},
{"name":"promo_categories","replication":{"checksum":-3856100130123609354,"data_count":2}},
{"name":"promo_events","replication":{"checksum":-2785478939128169531,"data_count":7}},
{"name":"promo_partners","replication":{"checksum":-7505835019945288960,"data_count":4}},
{"name":"providers","replication":{"checksum":-4628964894429265595,"data_count":7}},
{"name":"publication_statuses_dict","replication":{"checksum":0,"data_count":0}},
{"name":"purchase_groups","replication":{"checksum":-3584870434826938996,"data_count":8}},
{"name":"purchases_history","replication":{"checksum":8435102283672897229,"data_count":647}},
{"name":"radio_channels","replication":{"checksum":-4221151488359599036,"data_count":28}},
{"name":"recom_ab_test","replication":{"checksum":0,"data_count":0}},
{"name":"recom_cold_start_genres","replication":{"checksum":0,"data_count":0}},
{"name":"recom_cold_start_matrix","replication":{"checksum":0,"data_count":0}},
{"name":"recom_epg_archive_default","replication":{"checksum":806350292753475717,"data_count":5450}},
{"name":"recom_epg_archive_personal","replication":{"checksum":0,"data_count":0}},
{"name":"recom_epg_live_default","replication":{"checksum":333975289319231224,"data_count":10422}},
{"name":"recom_epg_live_personal","replication":{"checksum":0,"data_count":0}},
{"name":"recom_epg_mixed_default","replication":{"checksum":-534673942784874920,"data_count":5674}},
{"name":"recom_epg_mixed_personal","replication":{"checksum":0,"data_count":0}},
{"name":"recom_epg_vod_similar","replication":{"checksum":0,"data_count":0}},
{"name":"recom_media_items_default","replication":{"checksum":-9013114015125235173,"data_count":3}},
{"name":"recom_media_items_personal","replication":{"checksum":0,"data_count":0}},
{"name":"recom_media_items_recent_top","replication":{"checksum":2247183091106463653,"data_count":48}},
{"name":"recom_media_items_similars","replication":{"checksum":9135651007282691193,"data_count":187425}},
{"name":"recom_playlist_personal","replication":{"checksum":0,"data_count":0}},
{"name":"recorded_programs","replication":{"checksum":0,"data_count":0}},
{"name":"reminders","replication":{"checksum":2248263727952493366,"data_count":286}},
{"name":"screensavers","replication":{"checksum":9152039640054281746,"data_count":4}},
{"name":"sdp_locations","replication":{"checksum":-7665571899289048695,"data_count":89}},
{"name":"segments","replication":{"checksum":-4755635384362525758,"data_count":16}},
{"name":"service_purchases","replication":{"checksum":-6294335602006080101,"data_count":445631}},
{"name":"service_tabs","replication":{"checksum":4795178279284562198,"data_count":4}},
{"name":"service_types_dict","replication":{"checksum":-5267595763483467991,"data_count":3}},
{"name":"services","replication":{"checksum":-6430394930550856752,"data_count":9719}},
{"name":"sessions","replication":{"checksum":-6280667157063897303,"data_count":48}},
{"name":"sms_gateways_dict","replication":{"checksum":-1775234810599975284,"data_count":7}},
{"name":"sorts_vod","replication":{"checksum":8258846890794411103,"data_count":10}},
{"name":"splash_screens","replication":{"checksum":-4991365393737354657,"data_count":12}},
{"name":"subscription_requests","replication":{"checksum":4158684140272218076,"data_count":2227}},
{"name":"text_templates","replication":{"checksum":5176852717908120045,"data_count":73}},
{"name":"timezones","replication":{"checksum":-1427405116660937314,"data_count":12}},
{"name":"usage_models","replication":{"checksum":-4626611886390479362,"data_count":5}},
{"name":"vod_discounts","replication":{"checksum":9083302852446010667,"data_count":5}},
{"name":"vod_genres","replication":{"checksum":620338711206652696,"data_count":62}}
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
