#pragma once

#include "namespaceimpl.h"

namespace reindexer {

class [[nodiscard]] BackgroundNamespaceDeleter {
public:
	void Add(NamespaceImpl::Ptr ns) {
		lock_guard lck(mtx_);
		namespaces_.emplace_back(std::move(ns));
	}
	void DeleteUnique() noexcept {
		unique_lock lck(mtx_);
		for (auto it = namespaces_.begin(); it != namespaces_.end();) {
			if (it->unique()) {
				lck.unlock();
				it->reset();
				lck.lock();
				it = namespaces_.erase(it);
			} else {
				++it;
			}
		}
	}

private:
	mutex mtx_;
	elist<NamespaceImpl::Ptr> namespaces_;
};

}  // namespace reindexer
