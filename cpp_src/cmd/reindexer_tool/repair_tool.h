#pragma once

#include "core/storage/idatastorage.h"
#include "core/storage/storagetype.h"
#include "tools/errors.h"

namespace reindexer_tool {

using reindexer::datastorage::IDataStorage;
using reindexer::datastorage::StorageType;
using reindexer::Error;

class [[nodiscard]] RepairTool {
public:
	static Error RepairStorage(const std::string& dsn) noexcept;

private:
	static Error repairNamespace(IDataStorage* storage, const std::string& storagePath, const std::string& name,
								 StorageType storageType) noexcept;
};

}  // namespace reindexer_tool
