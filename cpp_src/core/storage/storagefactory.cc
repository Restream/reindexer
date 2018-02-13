#include "storagefactory.h"
#include "leveldbstorage.h"

namespace reindexer {
namespace datastorage {

IDataStorage* StorageFactory::create(StorageType type) {
	switch (type) {
		case StorageType::LevelDB:
			return new LevelDbStorage();
		default:
			throw std::runtime_error("No such storage type!");
	}
}
}  // namespace datastorage
}  // namespace reindexer
