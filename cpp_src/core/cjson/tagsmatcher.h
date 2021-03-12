#pragma once

#include <stdlib.h>
#include "core/cjson/tagsmatcherimpl.h"
#include "core/payload/payloadtype.h"
#include "estl/cow.h"
#include "tagspathcache.h"
#include "tools/serializer.h"

namespace reindexer {

class TagsMatcher {
public:
	TagsMatcher() : impl_(make_intrusive<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>()), updated_(false) {}
	TagsMatcher(PayloadType payloadType)
		: impl_(make_intrusive<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>(payloadType)), updated_(false) {}

	int name2tag(string_view name) const { return impl_->name2tag(name); }
	int name2tag(string_view name, bool canAdd) {
		if (!name.data()) return 0;
		int res = impl_->name2tag(name);
		return res ? res : impl_.clone()->name2tag(name, canAdd, updated_);
	}
	int tags2field(const int16_t* path, size_t pathLen) const { return impl_->tags2field(path, pathLen); }
	const string& tag2name(int tag) const { return impl_->tag2name(tag); }
	TagsPath path2tag(string_view jsonPath) const { return impl_->path2tag(jsonPath); }
	TagsPath path2tag(string_view jsonPath, bool canAdd) {
		auto res = path2tag(jsonPath);
		if (jsonPath.empty()) return TagsPath();
		return res.empty() && canAdd ? impl_.clone()->path2tag(jsonPath, canAdd, updated_) : res;
	}
	IndexedTagsPath path2indexedtag(string_view jsonPath, IndexExpressionEvaluator ev) const {
		IndexedTagsPath tagsPath = impl_->path2indexedtag(jsonPath, ev);
		assert(!updated_);
		return tagsPath;
	}
	IndexedTagsPath path2indexedtag(string_view jsonPath, IndexExpressionEvaluator ev, bool canAdd) {
		auto res = impl_->path2indexedtag(jsonPath, ev);
		if (jsonPath.empty()) return IndexedTagsPath();
		return res.empty() && canAdd ? impl_.clone()->path2indexedtag(jsonPath, ev, canAdd, updated_) : res;
	}
	int version() const { return impl_->version(); }
	size_t size() const { return impl_->size(); }
	bool isUpdated() const { return updated_; }
	uint32_t stateToken() const { return impl_->stateToken(); }
	void clear() { impl_.clone()->clear(); }
	void serialize(WrSerializer& ser) const { impl_->serialize(ser); }
	void deserialize(Serializer& ser) {
		impl_.clone()->deserialize(ser);
		impl_.clone()->buildTagsCache(updated_);
	}
	void deserialize(Serializer& ser, int version, int stateToken) {
		impl_.clone()->deserialize(ser, version, stateToken);
		impl_.clone()->buildTagsCache(updated_);
	}
	void clearUpdated() { updated_ = false; }
	void setUpdated() { updated_ = true; }

	bool try_merge(const TagsMatcher& tm) {
		auto tmp = impl_;
		if (!tmp.clone()->merge(*tm.impl_.get())) {
			return false;
		}
		impl_ = tmp;
		updated_ = true;
		return true;
	}

	void UpdatePayloadType(PayloadType payloadType, bool incVersion = true) {
		impl_.clone()->updatePayloadType(payloadType, updated_, incVersion);
	}

	string dump() const { return impl_->dumpTags() + "\n" + impl_->dumpPaths(); }

protected:
	shared_cow_ptr<TagsMatcherImpl> impl_;
	bool updated_;
};

}  // namespace reindexer
