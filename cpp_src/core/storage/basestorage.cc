#include "basestorage.h"
#include "tools/assertrx.h"
#include "tools/fsops.h"
#include "tools/logger.h"

namespace reindexer {

namespace datastorage {

using namespace std::string_view_literals;

const std::string kShutdownPlaceholderName = ".rdx_shutdown";
constexpr auto kLostDirName = "lost"sv;

BaseStorage::~BaseStorage() {
	std::lock_guard<std::mutex> lck(mtx_);
	if (info_ && info_.use_count() == 2) {
		DirsMap::GetInstance().TryRemoveInfoFromMap(info_->Path());
	}
}

Error BaseStorage::Open(const std::string& path, const StorageOpts& opts) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (!info_) {
		info_ = DirsMap::GetInstance().GetDirInfo(path, true);
	}
	std::lock_guard<std::mutex> pathLck(info_->mtx);
	assertrx(path == info_->Path());
	if (info_->IsDestroyed()) {
		info_->CreatePaceholder();
	}
	if (opts.IsAutorepair() && !info_->repaired) {
		info_->repaired = true;
		lck.unlock();
		logFmt(LogWarning, "Calling repair for '{}'", path);
		if (auto err = Repair(path); !err.ok()) {
			logFmt(LogError, "Rapir error: {}", err.what());
		}
	} else {
		lck.unlock();
	}
	return doOpen(path, opts);
}

void BaseStorage::Destroy(const std::string& path) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (!info_) {
		info_ = DirsMap::GetInstance().GetDirInfo(path);
		if (!info_) {
			return;
		}
	}

	std::unique_lock<std::mutex> pathLck(info_->mtx);
	assertrx(path == info_->Path());
	info_->RemovePlaceholder();
	fs::RmDirAll(fs::JoinPath(path, std::string(kLostDirName)));
	doDestroy(path);
	if (info_.use_count() == 2) {
		if (DirsMap::GetInstance().TryRemoveInfoFromMap(path)) {
			pathLck.unlock();  // Now this owns unique copy of DirInfo's shared_ptr
		}
	}
	info_.reset();
}

BaseStorage::DirectoryInfo::DirectoryInfo(const std::string& path) noexcept
	: placeholderPath_(fs::JoinPath(path, kShutdownPlaceholderName)), path_(path), requireRemove_(false) {
	repaired = (fs::Stat(placeholderPath_) == fs::StatError);
}

BaseStorage::DirectoryInfo::~DirectoryInfo() {
	std::lock_guard<std::mutex> lck(mtx);
	RemovePlaceholder();
}

void BaseStorage::DirectoryInfo::RemovePlaceholder() noexcept {
	if (requireRemove_) {
		remove(placeholderPath_.c_str());
		requireRemove_ = false;
	}
}

void BaseStorage::DirectoryInfo::CreatePaceholder() noexcept {
	if (fs::Stat(path_) == fs::StatError) {
		if (fs::MkDirAll(path_) < 0) {
			logFmt(LogWarning, "Unable to create directory for shutdown placeholder: {}", placeholderPath_);
		}
	}
	FILE* f = fopen(placeholderPath_.c_str(), "w");
	if (f) {
		fclose(f);
		requireRemove_ = true;
	} else {
		logFmt(LogWarning, "Unable to create shutdown placeholder: {}", placeholderPath_);
	}
}

}  // namespace datastorage
}  // namespace reindexer
