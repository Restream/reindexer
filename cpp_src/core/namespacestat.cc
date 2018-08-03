#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>

#include "namespacestat.h"
#include "tools/serializer.h"

#ifdef _WIN32
#define PRI_SIZE_T "Iu"
#else
#define PRI_SIZE_T "zu"
#endif

namespace reindexer {

void NamespaceMemStat::GetJSON(WrSerializer &ser) {
	ser.PutChar('{');

	ser.Printf("\"name\":\"%s\",", name.c_str());
	ser.Printf("\"items_count\":%" PRI_SIZE_T ",", itemsCount);

	if (emptyItemsCount) ser.Printf("\"empty_items_count\":%" PRI_SIZE_T ",", emptyItemsCount);

	ser.Printf("\"data_size\":%" PRI_SIZE_T ",", dataSize);

	ser.Printf("\"updated_unix_nano\":");
	ser.Print(int64_t(updatedUnixNano));
	ser.Printf(",\"storage_ok\":%s,", storageOK ? "true" : "false");
	ser.Printf("\"storage_path\":");
	ser.PrintJsonString(storagePath);
	ser.Printf(",");

	ser.Printf("\"total\":{");

	ser.Printf("\"data_size\":%" PRI_SIZE_T ",", Total.dataSize);
	ser.Printf("\"indexes_size\":%" PRI_SIZE_T ",", Total.indexesSize);
	ser.Printf("\"cache_size\":%" PRI_SIZE_T "", Total.cacheSize);

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

	ser.Printf("\"total_size\":%" PRI_SIZE_T ",", totalSize);
	ser.Printf("\"items_count\":%" PRI_SIZE_T ",", itemsCount);
	ser.Printf("\"empty_count\":%" PRI_SIZE_T ",", emptyCount);
	ser.Printf("\"hit_count_limit\":%" PRI_SIZE_T "", hitCountLimit);
	ser.PutChar('}');
}

void IndexMemStat::GetJSON(WrSerializer &ser) {
	ser.PutChar('{');

	if (uniqKeysCount) ser.Printf("\"uniq_keys_count\":%" PRI_SIZE_T ",", uniqKeysCount);
	if (dataSize) ser.Printf("\"data_size\":%" PRI_SIZE_T ",", dataSize);
	if (idsetBTreeSize) ser.Printf("\"idset_btree_size\":%" PRI_SIZE_T ",", idsetBTreeSize);
	if (idsetPlainSize) ser.Printf("\"idset_plain_size\":%" PRI_SIZE_T ",", idsetPlainSize);
	if (sortOrdersSize) ser.Printf("\"sort_orders_size\":%" PRI_SIZE_T ",", sortOrdersSize);
	if (fulltextSize) ser.Printf("\"fulltext_size\":%" PRI_SIZE_T ",", fulltextSize);
	if (columnSize) ser.Printf("\"column_size\":%" PRI_SIZE_T ",", columnSize);

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
	ser.Printf("\"total_queries_count\":%" PRI_SIZE_T ",", totalHitCount);
	ser.Printf("\"total_avg_latency_us\":%" PRI_SIZE_T ",", totalTimeUs);
	ser.Printf("\"total_avg_lock_time_us\":%" PRI_SIZE_T ",", totalLockTimeUs);
	ser.Printf("\"last_sec_qps\":%" PRI_SIZE_T ",", avgHitCount);
	ser.Printf("\"last_sec_avg_lock_time_us\":%" PRI_SIZE_T ",", avgLockTimeUs);
	ser.Printf("\"last_sec_avg_latency_us\":%" PRI_SIZE_T "", avgTimeUs);
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