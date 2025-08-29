#pragma once

#include "core/storage/idatastorage.h"
#include "tools/errors.h"

namespace reindexer_tool {

using reindexer::datastorage::IDataStorage;
using reindexer::Error;

class ConvertTool {
public:
	static Error ConvertStorage(std::string_view dsn, std::string_view convertFormat, std::string_view convertBackupFolder) noexcept;

private:
	static Error convertNamespace(IDataStorage* originalStorage, IDataStorage* convertedStorage, std::string_view originalPath,
								  std::string_view convertedPath, std::string_view name) noexcept;
};

}  // namespace reindexer_tool
