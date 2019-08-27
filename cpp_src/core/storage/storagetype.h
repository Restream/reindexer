#pragma once

#include "tools/errors.h"

namespace reindexer {
namespace datastorage {

enum class StorageType : uint8_t { LevelDB = 0, RocksDB = 1 };

const char kLevelDBName[] = "leveldb";
const char kRocksDBName[] = "rocksdb";

inline string StorageTypeToString(StorageType type) {
	if (StorageType::RocksDB == type) {
		return kRocksDBName;
	}
	return kLevelDBName;
}

inline StorageType StorageTypeFromString(string_view str) {
	if (str == kLevelDBName) {
		return StorageType::LevelDB;
	} else if (str == kRocksDBName) {
		return StorageType::RocksDB;
	} else {
		throw Error(errParams, "Invalid storage type string: %s", str);
	}
}

}  // namespace datastorage
}  // namespace reindexer
