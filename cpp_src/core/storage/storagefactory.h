#pragma once

#include "idatastorage.h"
#include "storagetype.h"

namespace reindexer {
namespace datastorage {

class StorageFactory {
public:
	static IDataStorage* create(StorageType);
	static std::vector<StorageType> getAvailableTypes();
};
}  // namespace datastorage
}  // namespace reindexer
