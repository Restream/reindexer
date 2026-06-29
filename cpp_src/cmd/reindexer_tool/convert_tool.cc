#include "convert_tool.h"
#include "core/reindexer.h"
#include "core/storage/storagefactory.h"
#include "core/system_ns_names.h"
#include "iotools.h"
#include "tools/catch_and_return.h"
#include "tools/fsops.h"
#include "tools/scope_guard.h"

namespace reindexer_tool {

using reindexer::datastorage::StorageType;

const char kStoragePlaceholderFilename[] = ".reindexer.storage";
const std::string kBuiltinPrefix = "builtin://";

namespace {

StorageTypeOpt getOpt(StorageType type) {
	if (type == StorageType::LevelDB) {
		return kStorageTypeOptLevelDB;
	}
	return kStorageTypeOptRocksDB;
}

}  // namespace

Error ConvertTool::ConvertStorage(std::string_view dsn, std::string_view convertFormat, std::string_view convertBackupFolder) noexcept {
	try {
		StorageType convertedStorageType = reindexer::datastorage::StorageTypeFromString(convertFormat);

		if (!dsn.starts_with(kBuiltinPrefix)) {
			return Error(errParams, "Invalid DSN format for convertation: {}. Must begin from {}", dsn, kBuiltinPrefix);
		}

		auto path = dsn.substr(kBuiltinPrefix.size());
		std::cout << "Starting database converting..." << std::endl;

		std::vector<std::string_view> pathParts;
		std::ignore = reindexer::split(path, "/", true, pathParts);

		if (pathParts.empty()) {
			return Error(errParams, "Invalid DSN format for convertation: {}. Empty database name.", dsn);
		}

		std::string_view dbName = pathParts.back();
		std::string backupFolder = std::string(convertBackupFolder);
		if (convertBackupFolder.empty()) {
			backupFolder = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindexer/convert_backup");
		}
		backupFolder = reindexer::fs::JoinPath(backupFolder, dbName);

		std::string tmpFolder;
		if (convertBackupFolder.empty()) {
			tmpFolder = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindexer/convert_tmp");
		} else {
			tmpFolder = std::string(convertBackupFolder) + "_tmp";
		}

		std::ignore = reindexer::fs::RmDirAll(backupFolder);
		std::ignore = reindexer::fs::RmDirAll(tmpFolder);

		std::cout << "Storing backup in " << backupFolder << std::endl;

		bool cleanupRequired = true;
		auto cleanup = reindexer::MakeScopeGuard([&cleanupRequired, &tmpFolder]() {
			if (cleanupRequired) {
				std::ignore = reindexer::fs::RmDirAll(tmpFolder);
			}
		});

		if (reindexer::fs::MkDirAll(backupFolder) < 0) {
			return {errParams, "Can't create directory '{}' to backup storage data: {}", backupFolder, strerror(errno)};
		}

		std::string content;
		std::string storagePlaceholderPath = reindexer::fs::JoinPath(std::string(path), kStoragePlaceholderFilename);
		int res = reindexer::fs::ReadFile(storagePlaceholderPath, content);
		if (res <= 0) {
			return Error(errParams, "'{}' - directory doesn't contain valid reindexer placeholder", path);
		}

		std::cout << "Found storage type: " << content << std::endl;
		if (content == convertFormat) {
			return Error(errParams, "Cant convert to: {}. Already stored in this format", convertFormat);
		}

		StorageType originalStorageType = reindexer::datastorage::StorageTypeFromString(content);

		std::string backupPlaceholderPath = reindexer::fs::JoinPath(backupFolder, kStoragePlaceholderFilename);
		int writeRes = reindexer::fs::WriteFile(backupPlaceholderPath, convertFormat);
		if (writeRes <= 0) {
			return {errSystem, "Failed to write storage type to placeholder '{}' : {}", backupPlaceholderPath, strerror(errno)};
		}

		// converting namespaces
		{
			std::vector<reindexer::fs::DirEntry> foundNs;
			if (reindexer::fs::ReadDir(std::string(path), foundNs) < 0) {
				return Error(errParams, "Can't read dir to convert: {}", path);
			}

			for (auto& ns : foundNs) {
				if (ns.name == reindexer::kEmbeddersPseudoNamespace) {
					// contains not only . and ..
					if (ns.internalFilesCount > 2) {
						std::cout << "Warning: " << reindexer::kEmbeddersPseudoNamespace
								  << " namespace can not be converted. Embedders cache will be lost after conversion." << std::endl;
					}
					break;
				}
			}

			for (auto& ns : foundNs) {
				if (!ns.isDir || !reindexer::validateObjectName(ns.name, true)) {
					continue;
				}

				std::unique_ptr<reindexer::datastorage::IDataStorage> originalStorage;
				std::unique_ptr<reindexer::datastorage::IDataStorage> convertedStorage;
				try {
					originalStorage.reset(reindexer::datastorage::StorageFactory::create(originalStorageType));
					convertedStorage.reset(reindexer::datastorage::StorageFactory::create(convertedStorageType));
				} catch (std::exception& ex) {
					return Error(errParams, "Skipping DB at '{}' - ", path, ex.what());
				}

				auto err = convertNamespace(originalStorage.get(), convertedStorage.get(), path, backupFolder, ns.name);
				if (!err.ok()) {
					std::cerr << "Converting error [" << reindexer::fs::JoinPath(std::string(path), ns.name) << "]: " << err.what()
							  << std::endl;
					return err;
				}
			}
		}

		// checking converted storage
		{
			reindexer::Reindexer ri;
			auto riOpts = ConnectOpts().WithStorageType(getOpt(convertedStorageType));
			if (Error err = ri.Connect(kBuiltinPrefix + backupFolder, riOpts); !err.ok()) {
				std::cerr << "Error loading reindexer on converted data [" << kBuiltinPrefix + backupFolder << "]: " << err.what()
						  << std::endl;
				return err;
			}
		}

		cleanupRequired = false;

		int renameRes = reindexer::fs::Rename(std::string(path), tmpFolder);
		if (renameRes < 0) {
			return Error(errParams, "Unable to rename folder [{}] to [{}]. Reason: {}", std::string(path), tmpFolder, strerror(errno));
		}

		renameRes = reindexer::fs::Rename(backupFolder, std::string(path));
		if (renameRes < 0) {
			Error err =
				Error(errParams, "Unable to rename folder [{}] to [{}]. Reason: {}", backupFolder, std::string(path), strerror(errno));
			[[maybe_unused]] int revertRenameRes = reindexer::fs::Rename(tmpFolder, std::string(path));
			return err;
		}

		renameRes = reindexer::fs::Rename(tmpFolder, backupFolder);
		if (renameRes < 0) {
			return Error(errParams, "Unable to rename folder [{}] to [{}]. Reason: {}", tmpFolder, backupFolder, strerror(errno));
		}

		cleanupRequired = true;
	}
	CATCH_AND_RETURN

	return errOK;
}

Error ConvertTool::convertNamespace(IDataStorage* originalStorage, IDataStorage* convertedStorage, std::string_view originalPath,
									std::string_view convertedPath, std::string_view name) noexcept {
	if (name == reindexer::kEmbeddersPseudoNamespace) {
		return errOK;
	}

	auto nsPath = reindexer::fs::JoinPath(std::string(originalPath), name);
	auto nsConvertedPath = reindexer::fs::JoinPath(std::string(convertedPath), name);

	if (reindexer::fs::MkDirAll(nsConvertedPath) < 0) {
		return {errParams, "Can't create directory '{}' for saving storage data: {}", nsConvertedPath, strerror(errno)};
	}

	std::cout << "Converting " << nsPath << " to " << nsConvertedPath << " ..." << std::endl;

	if (Error err = originalStorage->Open(nsPath, StorageOpts()); !err.ok()) {
		return err;
	}

	StorageOpts convertedOps;
	if (Error err = convertedStorage->Open(nsConvertedPath, convertedOps.CreateIfMissing()); !err.ok()) {
		return err;
	}

	StorageOpts cursorOpts;
	// read all keys from storage
	std::unique_ptr<reindexer::datastorage::Cursor> dbIter(originalStorage->GetCursor(cursorOpts));

	for (dbIter->SeekToFirst(); dbIter->Valid(); dbIter->Next()) {
		const auto storageKey = dbIter->Key();
		const auto storageValue = dbIter->Value();

		if (Error err = convertedStorage->Write(StorageOpts(), storageKey, storageValue); !err.ok()) {
			return err;
		}
	}

	return errOK;
}

}  // namespace reindexer_tool
