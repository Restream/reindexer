#pragma once

#include <string>

#ifdef kStorageANNCachePrefix
static_assert(false, "Redefinition of kStorageANNCachePrefix");
#endif	// kStorageANNCachePrefix
#define kStorageANNCachePrefix "ann_cache"

#ifdef kRxStorageItemPrefix
static_assert(false, "Redefinition of kRxStorageItemPrefix");
#endif	// kRxStorageItemPrefix
#define kRxStorageItemPrefix "I"

#ifdef kStorageWALPrefix
static_assert(false, "Redefinition of kStorageWALPrefix");
#endif	// kStorageWALPrefix
#define kStorageWALPrefix "W"

#ifdef kStorageHashTablesStatsPrefix
static_assert(false, "Redefinition of kStorageHashTablesStatsPrefix");
#endif	// kStorageHashTablesStatsPrefix
#define kStorageHashTablesStatsPrefix "hash_tables_stats"

namespace reindexer {
static const inline std::string kStorageMetaPrefix{"meta"};
}  // namespace reindexer