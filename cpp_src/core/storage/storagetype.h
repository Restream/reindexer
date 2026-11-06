#pragma once

#include <cctype>
#include "tools/errors.h"

namespace reindexer {
namespace datastorage {

enum class [[nodiscard]] StorageType : uint8_t { LevelDB = 0, RocksDB = 1 };

const char kLevelDBName[] = "leveldb";
const char kRocksDBName[] = "rocksdb";

inline std::string StorageTypeToString(StorageType type) {
	if (StorageType::RocksDB == type) {
		return kRocksDBName;
	}
	return kLevelDBName;
}

inline bool HasSpacesOnly(std::string_view str) noexcept {
	for (auto ch : str) {
		if (!std::isspace(ch)) {
			return false;
		}
	}
	return true;
}

inline StorageType StorageTypeFromString(std::string_view str) {
	if (str.empty()) {
		return StorageType::LevelDB;
	}
	if (str.substr(0, sizeof(kLevelDBName) - 1) == kLevelDBName && HasSpacesOnly(str.substr(sizeof(kLevelDBName) - 1))) {
		return StorageType::LevelDB;
	} else if (str.substr(0, sizeof(kRocksDBName) - 1) == kRocksDBName && HasSpacesOnly(str.substr(sizeof(kRocksDBName) - 1))) {
		return StorageType::RocksDB;
	} else {
		throw Error(errParams, "Invalid storage type string: '{}'", str);
	}
}

}  // namespace datastorage
}  // namespace reindexer
