#pragma once

#include "estl/fast_hash_set.h"
#include "namespacename.h"

namespace reindexer {

using NsNamesHashSetT = fast_hash_set<NamespaceName, NamespaceNameHash, NamespaceNameEqual, NamespaceNameLess>;
}  // namespace reindexer
