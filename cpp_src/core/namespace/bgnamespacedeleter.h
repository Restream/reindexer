#pragma once

#include "namespaceimpl.h"

namespace reindexer {

class BackgroundNamespaceDeleter {
public:
	void Add(NamespaceImpl::Ptr ns) {
		std::lock_guard lck(mtx_);
		namespaces_.emplace_back(std::move(ns));
	}
	void DeleteUnique() noexcept {
		std::unique_lock lck(mtx_);
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
	std::mutex mtx_;
	std::list<NamespaceImpl::Ptr> namespaces_;
};

}
