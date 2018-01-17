#pragma once

#include "core/cjson/tagsmatcherimpl.h"
#include "core/payload/payloadtype.h"
#include "estl/cow.h"
#include "tagspathcache.h"
#include "tools/serializer.h"

namespace reindexer {

class TagsMatcher {
public:
	TagsMatcher(PayloadType::Ptr payloadType = nullptr) : impl_(new TagsMatcherImpl(payloadType)), updated_(false) {}

	int name2tag(const char* name) const { return impl_->name2tag(name); }
	int name2tag(const string& name, const string& path) { return impl_.clone()->name2tag(name, path, updated_); }
	int name2tag(const char* name, bool canAdd) { return impl_.clone()->name2tag(name, canAdd, updated_); }
	int tags2field(const int* path, size_t pathLen) const { return impl_->tags2field(path, pathLen); }
	const string& tag2name(int tag) const { return impl_->tag2name(tag); }
	int version() const { return impl_->version(); }
	size_t size() const { return impl_->size(); }
	bool isUpdated() const { return updated_; }
	void clear() { impl_.clone()->clear(); }
	void serialize(WrSerializer& ser) const { impl_->serialize(ser); }
	void deserialize(Serializer& ser) {
		impl_.clone()->deserialize(ser);
		impl_.clone()->buildTagsCache(updated_);
	}
	void clearUpdated() { updated_ = false; }
	void setUpdated() { updated_ = true; }

	bool try_merge(const TagsMatcher& tm) {
		if (tm.isUpdated()) {
			auto tmp = impl_;
			if (!tmp.clone()->merge(*tm.impl_.get())) {
				return false;
			}
			impl_ = tmp;
			updated_ = true;
		}
		return true;
	}
	void updatePayloadType(PayloadType::Ptr payloadType) {
		auto old = impl_;
		impl_ = new TagsMatcherImpl(payloadType);
		impl_.clone()->merge(*old.get());
		impl_.clone()->buildTagsCache(updated_);
		updated_ = true;
	}
	string dump() { return impl_->dumpTags() + "\n" + impl_->dumpPaths(); }

protected:
	shared_cow_ptr<TagsMatcherImpl> impl_;
	bool updated_;
};

}  // namespace reindexer
