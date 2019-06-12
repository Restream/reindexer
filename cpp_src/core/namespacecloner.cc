#include "namespacecloner.h"
#include <memory>

namespace reindexer {

using std::lock_guard;

#if ATOMIC_NS_CLONE
template <typename _Tp>
std::shared_ptr<_Tp> atomic_load_sptr(const std::shared_ptr<_Tp> *__p) {
	return std::atomic_load(__p);
}
template <typename _Tp>
void atomic_exchange_sptr(std::shared_ptr<_Tp> *__p, std::shared_ptr<_Tp> __r) {
	std::atomic_exchange(__p, __r);
}

#else
template <typename _Tp>
std::shared_ptr<_Tp> atomic_load_sptr(const std::shared_ptr<_Tp> *__p) {
	return *__p;
}
template <typename _Tp>
void atomic_exchange_sptr(std::shared_ptr<_Tp> *__p, std::shared_ptr<_Tp> __r) {
	__p->swap(__r);
}
#endif

NamespaceCloner::NamespaceCloner(Namespace::Ptr ptr)
	: writeQueueSize_(0), writeCompleteSize_(0), nmActiveCont_(0), notified_(0), originNs_(ptr) {
	if (ptr) {
		startCopyPoliticsCount = ptr->config_.startCopyPoliticsCount;
		mergeLimitCount = ptr->config_.mergeLimitCount;
	} else {
		NamespaceConfigData cfg;
		startCopyPoliticsCount = cfg.startCopyPoliticsCount;
		mergeLimitCount = cfg.mergeLimitCount;
	}
}

NamespaceCloner::~NamespaceCloner() { assert(!clonedNs_); }

void NamespaceCloner::MergeWriteNs() {
	// double check here - we never lock if no Merge need
	// Only one thread will merge after lock

	if (!needMerge()) return;
	lock_guard<shared_timed_mutex> lock(getOriginNsMutex());
	shared_lock<shared_timed_mutex> Wlock(getClonedNsMutex());
	if (!needMerge()) return;
	originNs_->CopyContentsFrom(*clonedNs_);

	Notify();
}

bool NamespaceCloner::needMerge() {
	if (writeCompleteSize_ <= mergeLimitCount || mergeLimitCount == 0) return false;
	return true;
}

Namespace::Ptr NamespaceCloner::CloneNsIfNeed() {
	auto test = atomic_load_sptr(&clonedNs_);
	if (!needClone() || test) return test;
	lock_guard<shared_timed_mutex> lock(getOriginNsMutex());
	test = atomic_load_sptr(&clonedNs_);
	if (!needClone() || test || originNs_->repl_.slaveMode) return test;

	atomic_exchange_sptr(&clonedNs_, shared_ptr<Namespace>(new Namespace(*originNs_.get())));

	return atomic_load_sptr(&clonedNs_);
}

void NamespaceCloner::Notify() {
	// TODO - in real we don't need mutex here -  we can put while - but not now
	unique_lock<std::mutex> lk(cvm_);
	notified_ = nmActiveCont_;
	nmActiveCont_ = 0;
	cv_.notify_all();
}

void NamespaceCloner::Wait() {
	unique_lock<std::mutex> lk(cvm_);
	auto num = ++nmActiveCont_;
	while (num < notified_) {
		cv_.wait(lk);
	}
}

void NamespaceCloner::CloseClonedNs() {
	if (clonedNs_.use_count() != 1) return;
	lock_guard<shared_timed_mutex> lock(getOriginNsMutex());
	if (clonedNs_.use_count() != 1) return;

	auto wrNmTest = clonedNs_;
	atomic_exchange_sptr(&clonedNs_, {});

	// Some one take clonedNs_  (take no lock) after  check use count and before
	// reset - it is not good, but not a problem - nobody else can't take it
	// (clonedNs_ is reseted) just wait a couple moment's before the last one
	// free it before Notify () and real close
	unsigned i = 0;
	while (wrNmTest.use_count() != 1) {
		i++;
		if (i % 10000 == 0) std::this_thread::yield();
	}
	originNs_->MoveContentsFrom(move(*wrNmTest.get()));

	atomic_exchange_sptr(&wrNmTest, {});

	Notify();
}

void NamespaceCloner::AddSize(size_t size) { writeQueueSize_.fetch_add(size); }
void NamespaceCloner::SubSize(size_t size) {
	writeCompleteSize_ += size;
	writeQueueSize_ -= size;
}

Namespace::Ptr NamespaceCloner::GetOriginNs() { return atomic_load_sptr(&originNs_); }
Namespace::Ptr NamespaceCloner::GetClonedNs() { return atomic_load_sptr(&clonedNs_); }

shared_timed_mutex &NamespaceCloner::getOriginNsMutex() { return originNs_->mtx_; }
shared_timed_mutex &NamespaceCloner::getClonedNsMutex() { return clonedNs_->mtx_; }

bool NamespaceCloner::needClone() {
	uint32_t itemCount = originNs_->GetItemsCount();
	if (itemCount < startCopyPoliticsCount || startCopyPoliticsCount == 0) return false;
	if (writeQueueSize_.load() > startCopyPoliticsCount / 2) return true;
	return false;
}

ClonableNamespace::ClonableNamespace(size_t size, NamespaceCloner::Ptr nsWarpper) : size_(size), nsCloner_(nsWarpper) {
	nsCloner_->AddSize(size_);
	clonedNs_ = nsCloner_->GetClonedNs();
	// if we try to get and have two copy's (one here and one in static wrapper)
	// then we work in dual nm mode
	if (clonedNs_ && clonedNs_.use_count() >= 2) {
		nsCloner_->MergeWriteNs();
		return;
	} else if (!clonedNs_) {
		// no write ns at moment try to create it - it  be return null pointer
		// if no need -
		// then we work in single nm mode, if return not null dual nm mode
		clonedNs_ = nsCloner_->CloneNsIfNeed();
	} else {
		// in moment we try to take ptr before atomic increze - someone close it
		// just fogot it and work in sigle nm mode
		clonedNs_.reset();
	}
}

ClonableNamespace::~ClonableNamespace() {
	if (!nsCloner_) return;
	nsCloner_->SubSize(size_);

	// if we work in single nm mode
	if (!clonedNs_) return;
	// check if it last in order try to close
	if (clonedNs_.use_count() == 2) {
		atomic_exchange_sptr(&clonedNs_, {});
		nsCloner_->CloseClonedNs();
		return;
	}
	nsCloner_->Wait();
}

Namespace *ClonableNamespace::operator->() const noexcept {
	if (clonedNs_) return clonedNs_.get();
	return nsCloner_->GetOriginNs().get();
}
ClonableNamespace::operator bool() { return bool(nsCloner_->GetOriginNs()); }

}  // namespace reindexer
