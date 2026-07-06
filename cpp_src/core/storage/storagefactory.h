#pragma once

#include <vector>
#include "idatastorage.h"
#include "storagetype.h"

namespace reindexer {
namespace datastorage {

class [[nodiscard]] StorageFactory {
public:
	static IDataStorage* create(StorageType);
	static IDataStorage* create(std::string_view type);
	static std::vector<StorageType> getAvailableTypes();
};
}  // namespace datastorage
}  // namespace reindexer
