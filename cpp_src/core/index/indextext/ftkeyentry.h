#pragma once

#include "core/index/keyentry.h"
namespace reindexer {

class FtKeyEntryData : public KeyEntry<IdSetPlain> {
public:
	int vdoc_id_ = ndoc;
	static constexpr int ndoc = -1;

	int& VDocID() { return vdoc_id_; }
	const int& VDocID() const { return vdoc_id_; }
	FtKeyEntryData* get() { return this; }
	const FtKeyEntryData* get() const { return this; }
};

// IndexText need stable pointers to KeyEntry - they are used for expand idset, returned by Select from vdocs
// IndexUnordered is using flat_hash_map, therefore references to KeyEntry can be invalidated on any data update
// FtKeyEntry is unique_ptr<FtKeyEntryData> wrapper for storing stable pointer to actual KeyEntry

class FtKeyEntry {
public:
	FtKeyEntry() : impl_(new FtKeyEntryData){};
	FtKeyEntry(const FtKeyEntry& other) : impl_(other.impl_ ? new FtKeyEntryData(*other.impl_.get()) : nullptr) {}
	FtKeyEntry& operator=(const FtKeyEntry& other) {
		if (this != &other) {
			impl_.reset(other.impl_ ? new FtKeyEntryData(*other.impl_.get()) : nullptr);
		}
		return *this;
	}
	FtKeyEntry(FtKeyEntry&& /*other*/) noexcept = default;
	FtKeyEntry& operator=(FtKeyEntry&& /*other*/) noexcept = default;

	IdSetPlain& Unsorted() { return impl_->Unsorted(); }
	const IdSetPlain& Unsorted() const { return impl_->Unsorted(); }
	IdSetRef Sorted(unsigned sortId) const { return impl_->Sorted(sortId); }
	void UpdateSortedIds(const UpdateSortedContext& ctx) { impl_->UpdateSortedIds(ctx); }
	int& VDocID() { return impl_->vdoc_id_; }
	const int& VDocID() const { return impl_->vdoc_id_; }
	FtKeyEntryData* get() { return impl_.get(); }
	const FtKeyEntryData* get() const { return impl_.get(); }

protected:
	std::unique_ptr<FtKeyEntryData> impl_;
};

}  // namespace reindexer
