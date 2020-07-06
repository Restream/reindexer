#pragma once

#include "tools/errors.h"

namespace reindexer {
namespace datastorage {

enum class StorageType : uint8_t { LevelDB = 0, RocksDB = 1 };

const char kLevelDBName[] = "leveldb";
const char kRocksDBName[] = "rocksdb";

inline std::string StorageTypeToString(StorageType type) {
	if (StorageType::RocksDB == type) {
		return kRocksDBName;
	}
	return kLevelDBName;
}

inline StorageType StorageTypeFromString(string_view str) {
	if (str.empty()) {
		return StorageType::LevelDB;
	}
	if (str.substr(0, sizeof(kLevelDBName) - 1) == kLevelDBName) {
		return StorageType::LevelDB;
	} else if (str.substr(0, sizeof(kRocksDBName) - 1) == kRocksDBName) {
		return StorageType::RocksDB;
	} else {
		throw Error(errParams, "Invalid storage type string: %s", str);
	}
}

}  // namespace datastorage
}  // namespace reindexer
