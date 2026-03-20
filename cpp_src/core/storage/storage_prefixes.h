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

namespace reindexer {
constexpr inline std::string_view kStorageIndexesPrefix{"indexes"};
constexpr inline std::string_view kStorageSchemaPrefix{"schema"};
constexpr inline std::string_view kStorageReplStatePrefix{"repl"};
constexpr inline std::string_view kStorageTagsPrefix{"tags"};

static const inline std::string kStorageMetaPrefix{"meta"};
}  // namespace reindexer