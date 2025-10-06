#pragma once

#include "estl/fast_hash_map.h"
#include "namespacename.h"

namespace reindexer {

template <typename T>
using NsNamesHashMapT = fast_hash_map<NamespaceName, T, NamespaceNameHash, NamespaceNameEqual, NamespaceNameLess>;

}  // namespace reindexer
