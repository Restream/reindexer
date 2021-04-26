#include "storagefactory.h"
#include "leveldbstorage.h"
#include "rocksdbstorage.h"

namespace reindexer {
namespace datastorage {

IDataStorage* StorageFactory::create(StorageType type) {
	switch (type) {
		case StorageType::LevelDB:
#ifdef REINDEX_WITH_LEVELDB
			return new LevelDbStorage();
#else	// REINDEX_WITH_LEVELDB
			throw std::runtime_error("No such storage type!");
#endif	// REINDEX_WITH_LEVELDB
		case StorageType::RocksDB:
#ifdef REINDEX_WITH_ROCKSDB
			return new RocksDbStorage();
#else	// REINDEX_WITH_ROCKSDB
			throw std::runtime_error("No such storage type!");
#endif	// REINDEX_WITH_ROCKSDB
		default:
			throw std::runtime_error("No such storage type!");
	}
}

IDataStorage* create(std::string_view type) { return StorageFactory::create(StorageTypeFromString(type)); }

std::vector<StorageType> StorageFactory::getAvailableTypes() {
	std::vector<StorageType> types;
#ifdef REINDEX_WITH_LEVELDB
	types.emplace_back(StorageType::LevelDB);
#endif	// REINDEX_WITH_LEVELDB
#ifdef REINDEX_WITH_ROCKSDB
	types.emplace_back(StorageType::RocksDB);
#endif	// REINDEX_WITH_ROCKSDB
	return types;
}

}  // namespace datastorage
}  // namespace reindexer
