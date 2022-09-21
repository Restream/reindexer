#include "repair_tool.h"
#include <cctype>
#include "core/namespace/namespaceimpl.h"
#include "core/storage/storagefactory.h"
#include "iotools.h"
#include "tools/fsops.h"

namespace reindexer_tool {

const char kStoragePlaceholderFilename[] = ".reindexer.storage";
constexpr unsigned kStorageLoadingThreads = 6;

Error RepairTool::RepairStorage(const std::string& dsn) noexcept {
	if (dsn.compare(0, 10, "builtin://") != 0) {
		return Error(errParams, "Invalid DSN format for repair: %s. Must begin from builtin://", dsn);
	}

	std::cout << "Starting database repair..." << std::endl;
	auto path = dsn.substr(10);
	bool hasErrors = false;
	auto storageType = reindexer::datastorage::StorageType::LevelDB;
	std::string content;
	int res = reindexer::fs::ReadFile(reindexer::fs::JoinPath(path, kStoragePlaceholderFilename), content);
	if (res > 0) {
		std::unique_ptr<reindexer::datastorage::IDataStorage> storage;
		try {
			storage.reset(reindexer::datastorage::StorageFactory::create(storageType));
		} catch (std::exception& ex) {
			return Error(errParams, "Skiping DB at '%s' - ", path, ex.what());
		}
		vector<reindexer::fs::DirEntry> foundNs;
		if (reindexer::fs::ReadDir(path, foundNs) < 0) {
			return Error(errParams, "Can't read dir to repair: %s", path);
		}
		for (auto& ns : foundNs) {
			if (ns.isDir && reindexer::validateObjectName(ns.name, true)) {
				auto err = repairNamespace(storage.get(), path, ns.name, storageType);
				if (!err.ok()) {
					hasErrors = true;
					std::cerr << "Repair error [" << reindexer::fs::JoinPath(path, ns.name) << "]: " << err.what() << std::endl;
				}
			}
		}
	} else {
		return Error(errParams, "'%s' - directory doesn't contain valid reindexer placeholder", path);
	}

	return hasErrors ? Error(errParams, "Some of namespaces had repair errors") : errOK;
}

Error RepairTool::repairNamespace(IDataStorage* storage, const std::string& storagePath, const std::string& name,
								  StorageType storageType) noexcept {
	auto nsPath = reindexer::fs::JoinPath(storagePath, name);
	std::cout << "Repairing " << nsPath << "..." << std::endl;
	auto err = storage->Repair(nsPath);
	if (!err.ok()) {
		return err;
	}

	try {
		if (!reindexer::validateObjectName(name, true)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		reindexer::NamespaceImpl ns(name, {}, nullptr);
		StorageOpts storageOpts;
		reindexer::RdxContext dummyCtx;
		std::cout << "Loading " << name << std::endl;
		ns.EnableStorage(storagePath, storageOpts.Enabled(true), storageType, dummyCtx);
		ns.LoadFromStorage(kStorageLoadingThreads, dummyCtx);
	} catch (const Error& err) {
		std::cout << "Namespace was not repaired: " << err.what() << ". Should it be deleted? y/N" << std::endl;
		for (;;) {
			std::string input;
			std::getline(std::cin, input);
			std::transform(input.begin(), input.end(), input.begin(), [](char c) { return std::tolower(c); });
			if (input == "y" || input == "yes") {
				auto res = reindexer::fs::RmDirAll(nsPath);
				if (res < 0) {
					std::cerr << "Namespace rm error[" << nsPath << "]: %s" << strerror(errno) << std::endl;
				}
				break;
			} else if (input == "n" || input == "no" || input.empty()) {
				break;
			} else {
				std::cerr << "Expect 'yes' or 'no'" << std::endl;
			}
		}
	}

	return errOK;
}

}  // namespace reindexer_tool
