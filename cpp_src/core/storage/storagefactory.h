#pragma once

#include "idatastorage.h"

namespace reindexer {
namespace datastorage {

enum class StorageType { LevelDB = 0 };

class StorageFactory {
public:
	static IDataStorage* create(StorageType);
};
}  // namespace datastorage
}  // namespace reindexer
