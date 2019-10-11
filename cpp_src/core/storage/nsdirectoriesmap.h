#pragma once

#include <memory>
#include <unordered_map>
#include "estl/shared_mutex.h"
#include "estl/smart_lock.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace datastorage {

template <typename T>
class NsDirectoriesMap {
public:
	static NsDirectoriesMap<T>& GetInstance() {
		static NsDirectoriesMap<T> instance;
		return instance;
	}

	bool TryRemoveInfoFromMap(const std::string& path) noexcept {
		std::lock_guard<std::mutex> lck(mtx_);
		auto found = nsDirs_.find(path);
		if (found != nsDirs_.end() && found->second.use_count() == 2) {
			nsDirs_.erase(found);
			return true;
		}
		return false;
	}
	std::shared_ptr<T> GetDirInfo(const std::string& path, bool create = false) noexcept {
		std::lock_guard<std::mutex> lck(mtx_);
		auto found = nsDirs_.find(path);
		if (found != nsDirs_.end()) {
			return found->second;
		}

		if (create) {
			auto res = nsDirs_.emplace(path, std::make_shared<T>(path));
			return res.first->second;
		}
		return nullptr;
	}

private:
	std::mutex mtx_;
	std::unordered_map<string, std::shared_ptr<T>, nocase_hash_str, nocase_equal_str> nsDirs_;
};

}  // namespace datastorage
}  // namespace reindexer
