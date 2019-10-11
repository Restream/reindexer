#pragma once

#include <mutex>
#include "idatastorage.h"
#include "nsdirectoriesmap.h"

namespace reindexer {

namespace datastorage {

class BaseStorage : public IDataStorage {
public:
	virtual ~BaseStorage();

	Error Open(const string& path, const StorageOpts& opts) override final;
	void Destroy(const string& path) override final;

protected:
	/// Open implementation
	/// @param path - path to storage.
	/// @param opts - options.
	/// @return Error object with an appropriate error code.
	virtual Error doOpen(const string& path, const StorageOpts& opts) = 0;

	/// Destroy implementation
	/// @param path - path to Storage.
	virtual void doDestroy(const string& path) = 0;

private:
	class DirectoryInfo {
	public:
		DirectoryInfo(const std::string& placeholderPath) noexcept;
		~DirectoryInfo();

		void RemovePlaceholder() noexcept;
		void CreatePaceholder() noexcept;
		bool IsDestroyed() const { return !requireRemove_; }
		const std::string& Path() const { return path_; }

		std::mutex mtx;
		bool repaired;

	private:
		std::string placeholderPath_;
		std::string path_;
		bool requireRemove_;
	};

	using DirsMap = NsDirectoriesMap<BaseStorage::DirectoryInfo>;

	std::shared_ptr<DirectoryInfo> info_;
	std::mutex mtx_;
};

}  // namespace datastorage
}  // namespace reindexer
