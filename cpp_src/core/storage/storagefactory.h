#pragma once

#include "idatastorage.h"
#include "storagetype.h"

namespace reindexer {
namespace datastorage {

class StorageFactory {
public:
	static IDataStorage* create(StorageType);
	static IDataStorage* create(string_view type);
	static std::vector<StorageType> getAvailableTypes();
};
}  // namespace datastorage
}  // namespace reindexer
