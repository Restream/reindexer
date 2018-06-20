
#include "namespacestat.h"
#include "tools/serializer.h"

namespace reindexer {

void NamespaceMemStat::GetJSON(WrSerializer &ser) {
	ser.PutChar('{');

	ser.Printf("\"name\":\"%s\",", name.c_str());
	ser.Printf("\"items_count\":%zu,", itemsCount);

	if (emptyItemsCount) ser.Printf("\"empty_items_count\":%zu,", emptyItemsCount);

	ser.Printf("\"data_size\":%zu,", dataSize);

	ser.Printf("\"updated_unix_nano\":%llu,", updatedUnixNano);
	ser.Printf("\"storage_ok\":%s,", storageOK ? "true" : "false");
	ser.Printf("\"storage_path\":\"%s\",", storagePath.c_str());

	ser.Printf("\"total\":{");

	ser.Printf("\"data_size\":%zu,", Total.dataSize);
	ser.Printf("\"indexes_size\":%zu,", Total.indexesSize);
	ser.Printf("\"cache_size\":%zu", Total.cacheSize);

	ser.Printf("},\"join_cache\":");
	joinCache.GetJSON(ser);
	ser.Printf(",\"query_cache\":");
	queryCache.GetJSON(ser);
	ser.Printf(",\"indexes\":[");
	for (unsigned i = 0; i < indexes.size(); i++) {
		if (i != 0) ser.PutChar(',');
		indexes[i].GetJSON(ser);
	}

	ser.PutChars("]}");
};

void LRUCacheMemStat::GetJSON(WrSerializer &ser) {
	ser.PutChar('{');

	ser.Printf("\"total_size\":%zu,", totalSize);
	ser.Printf("\"items_count\":%zu,", itemsCount);
	ser.Printf("\"empty_count\":%zu,", emptyCount);
	ser.Printf("\"hit_count_limit\":%zu", hitCountLimit);
	ser.PutChar('}');
}

void IndexMemStat::GetJSON(WrSerializer &ser) {
	ser.PutChar('{');

	if (uniqKeysCount) ser.Printf("\"uniq_keys_count\":%zu,", uniqKeysCount);
	if (dataSize) ser.Printf("\"data_size\":%zu,", dataSize);
	if (idsetBTreeSize) ser.Printf("\"idset_btree_size\":%zu,", idsetBTreeSize);
	if (idsetPlainSize) ser.Printf("\"idset_plain_size\":%zu,", idsetPlainSize);
	if (sortOrdersSize) ser.Printf("\"sort_orders_size\":%zu,", sortOrdersSize);
	if (fulltextSize) ser.Printf("\"fulltext_size\":%zu,", fulltextSize);
	if (columnSize) ser.Printf("\"column_size\":%zu,", columnSize);

	if (idsetCache.totalSize || idsetCache.itemsCount || idsetCache.emptyCount || idsetCache.hitCountLimit) {
		ser.Printf("\"idset_cache\":");
		idsetCache.GetJSON(ser);
		ser.PutChar(',');
	}

	ser.Printf("\"name\":\"%s\"", name.c_str());

	ser.PutChar('}');
}
void PerfStat::GetJSON(WrSerializer &ser) {
	ser.PutChar('{');
	ser.Printf("\"total_queries_count\":%zu,", totalHitCount);
	ser.Printf("\"total_avg_latency_us\":%zu,", totalTimeUs);
	ser.Printf("\"total_avg_lock_time_us\":%zu,", totalLockTimeUs);
	ser.Printf("\"last_sec_qps\":%zu,", avgHitCount);
	ser.Printf("\"last_sec_avg_lock_time_us\":%zu,", avgLockTimeUs);
	ser.Printf("\"last_sec_avg_latency_us\":%zu", avgTimeUs);
	ser.PutChar('}');
}

void NamespacePerfStat::GetJSON(WrSerializer &ser) {
	ser.PutChar('{');
	ser.Printf("\"name\":\"%s\",", name.c_str());
	ser.Printf("\"updates\":");
	updates.GetJSON(ser);
	ser.Printf(",\"selects\":");
	selects.GetJSON(ser);
	ser.PutChar('}');
}

}  // namespace reindexer
