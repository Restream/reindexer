#pragma once

#include "core/index/keyentry.h"
namespace reindexer {

class [[nodiscard]] FtKeyEntryData : public KeyEntry<IdSetPlain> {
	using Base = KeyEntry<IdSetPlain>;
	friend class FtKeyEntry;

public:
	static constexpr int ndoc = -1;

	void SetVDocID(int vdoc_id) noexcept { vdoc_id_ = vdoc_id; }
	const int& VDocID() const noexcept { return vdoc_id_; }
	FtKeyEntryData* get() { return this; }
	const FtKeyEntryData* get() const { return this; }
	void Dump(std::ostream& os, std::string_view step, std::string_view offset) const;

private:
	int vdoc_id_ = ndoc;  // index in array IDataHolder::vdocs_
};

// IndexText need stable pointers to KeyEntry - they are used for expand idset, returned by Select from vdocs
// IndexUnordered is using flat_hash_map, therefore references to KeyEntry can be invalidated on any data update
// FtKeyEntry is unique_ptr<FtKeyEntryData> wrapper for storing stable pointer to actual KeyEntry

class [[nodiscard]] FtKeyEntry {
public:
	FtKeyEntry() : impl_(new FtKeyEntryData) {}
	FtKeyEntry(const FtKeyEntry& other) : impl_(other.impl_ ? new FtKeyEntryData(*other.impl_.get()) : nullptr) {}
	FtKeyEntry& operator=(const FtKeyEntry& other) {
		if (this != &other) {
			impl_.reset(other.impl_ ? new FtKeyEntryData(*other.impl_.get()) : nullptr);
		}
		return *this;
	}
	FtKeyEntry(FtKeyEntry&& /*other*/) noexcept = default;
	FtKeyEntry& operator=(FtKeyEntry&& /*other*/) noexcept = default;

	IdSetPlain& Unsorted() noexcept { return impl_->Unsorted(); }
	const IdSetPlain& Unsorted() const noexcept { return impl_->Unsorted(); }
	IdSetRef Sorted(unsigned sortId) const noexcept { return impl_->Sorted(sortId); }
	void UpdateSortedIds(const IUpdateSortedContext& ctx) { impl_->UpdateSortedIds(ctx); }
	void SetVDocID(int vdoc_id) noexcept { impl_->SetVDocID(vdoc_id); }
	const int& VDocID() const { return impl_->vdoc_id_; }
	FtKeyEntryData* get() { return impl_.get(); }
	const FtKeyEntryData* get() const { return impl_.get(); }
	void Dump(std::ostream& os, std::string_view step, std::string_view offset) const {
		assertrx(impl_);
		impl_->Dump(os, step, offset);
	}

protected:
	std::unique_ptr<FtKeyEntryData> impl_;
};

}  // namespace reindexer
