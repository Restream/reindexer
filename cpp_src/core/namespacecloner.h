#pragma once
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include "namespace.h"

namespace reindexer {

class NamespaceCloner {
public:
	typedef shared_ptr<NamespaceCloner> Ptr;

	NamespaceCloner(Namespace::Ptr ptr);
	~NamespaceCloner();

	void MergeWriteNs();

	Namespace::Ptr CloneNsIfNeed();

	void Notify();
	void Wait();

	void CloseClonedNs();

	void AddSize(size_t size);
	void SubSize(size_t size);

	Namespace::Ptr GetOriginNs();
	Namespace::Ptr GetClonedNs();

private:
	shared_timed_mutex &getOriginNsMutex();
	shared_timed_mutex &getClonedNsMutex();

	bool needClone();
	bool needMerge();

	std::atomic<uint32_t> writeQueueSize_;
	std::atomic<uint32_t> writeCompleteSize_;

	uint32_t nmActiveCont_;
	uint32_t notified_;

	// mutex only for condition_variable
	std::mutex cvm_;
	std::condition_variable cv_;

	Namespace::Ptr originNs_;
	Namespace::Ptr clonedNs_;
	uint32_t startCopyPoliticsCount;
	uint32_t mergeLimitCount;
};

/// ClonableNamespace - Lock free async wrapper
/// if we have a lot of task to get namespace for write
/// we create a copy here and then close it when order complete
class ClonableNamespace {
public:
	ClonableNamespace(size_t size, NamespaceCloner::Ptr nsWarpper);
	ClonableNamespace() {}

	~ClonableNamespace();
	Namespace *operator->() const noexcept;
	operator bool();

	ClonableNamespace(ClonableNamespace &&) = default;
	ClonableNamespace &operator=(ClonableNamespace &&) noexcept = default;

	ClonableNamespace(ClonableNamespace &) = delete;
	ClonableNamespace &operator=(ClonableNamespace &) noexcept = delete;

private:
	size_t size_;

	NamespaceCloner::Ptr nsCloner_;
	Namespace::Ptr clonedNs_;
};
}  // namespace reindexer
