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
	TagsMatcher() : impl_(std::make_shared<TagsMatcherImpl>()), updated_(false) {}
	TagsMatcher(PayloadType payloadType) : impl_(std::make_shared<TagsMatcherImpl>(payloadType)), updated_(false) {}

	int name2tag(const char* name) const { return impl_->name2tag(name); }
	int name2tag(const string& name, const string& path) { return impl_.clone()->name2tag(name, path, updated_); }
	int name2tag(const char* name, bool canAdd) {
		if (!name || !*name) return 0;
		int res = impl_->name2tag(name);
		return res ? res : impl_.clone()->name2tag(name, canAdd, updated_);
	}
	int tags2field(const int16_t* path, size_t pathLen) const { return impl_->tags2field(path, pathLen); }
	const string& tag2name(int tag) const { return impl_->tag2name(tag); }
	TagsPath path2tag(const string& jsonPath) const { return impl_->path2tag(jsonPath); }
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
	TagsPath path2tag(const std::string& jsonPath, bool& updated) {
		updated = false;
		string field;
		TagsPath tagsPath;
		for (size_t pos = 0, lastPos = 0; pos != jsonPath.length(); lastPos = pos + 1) {
			pos = jsonPath.find(".", lastPos);
			if (pos == string::npos) {
				pos = jsonPath.length();
			}
			if (pos != lastPos) {
				field.assign(jsonPath.data() + lastPos, pos - lastPos);
				int fieldTag = name2tag(field.c_str());
				if (fieldTag == 0) {
					fieldTag = name2tag(field.c_str(), true);
					updated = true;
				}
				tagsPath.push_back(static_cast<int16_t>(fieldTag));
			}
		}
		return tagsPath;
	}

	void updatePayloadType(PayloadType payloadType, bool incVersion = true) {
		impl_.clone()->updatePayloadType(payloadType, updated_, incVersion);
	}

	string dump() const { return impl_->dumpTags() + "\n" + impl_->dumpPaths(); }

protected:
	shared_cow_ptr<TagsMatcherImpl> impl_;
	bool updated_;
};

}  // namespace reindexer
