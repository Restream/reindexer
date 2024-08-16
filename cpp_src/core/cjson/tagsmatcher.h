#pragma once

#include <stdlib.h>
#include "core/cjson/tagsmatcherimpl.h"
#include "core/payload/payloadtype.h"
#include "estl/cow.h"
#include "tools/randomgenerator.h"
#include "tools/serializer.h"

namespace reindexer {

class TagsMatcher {
public:
	struct unsafe_empty_t {};

	TagsMatcher() : impl_(make_intrusive<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>()), updated_(false) {}
	TagsMatcher(unsafe_empty_t) noexcept : updated_(false) {}
	TagsMatcher(PayloadType payloadType, int32_t stateToken = tools::RandomGenerator::gets32())
		: impl_(make_intrusive<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>(std::move(payloadType), stateToken)), updated_(false) {}

	int name2tag(std::string_view name) const { return impl_->name2tag(name); }
	int name2tag(std::string_view name, bool canAdd) {
		if (!name.data()) {
			return 0;  // -V547
		}
		const int res = impl_->name2tag(name);
		return res ? res : impl_.clone()->name2tag(name, canAdd, updated_);
	}
	int tags2field(const int16_t* path, size_t pathLen) const noexcept { return impl_->tags2field(path, pathLen); }
	const std::string& tag2name(int tag) const { return impl_->tag2name(tag); }
	TagsPath path2tag(std::string_view jsonPath) const { return impl_->path2tag(jsonPath); }
	TagsPath path2tag(std::string_view jsonPath, bool canAdd) {
		if (jsonPath.empty()) {
			return TagsPath();
		}
		auto res = path2tag(jsonPath);
		return res.empty() && canAdd ? impl_.clone()->path2tag(jsonPath, canAdd, updated_) : res;
	}
	IndexedTagsPath path2indexedtag(std::string_view jsonPath) const {
		IndexedTagsPath tagsPath = impl_->path2indexedtag(jsonPath);
		assertrx(!updated_);
		return tagsPath;
	}
	IndexedTagsPath path2indexedtag(std::string_view jsonPath, bool canAdd) {
		if (jsonPath.empty()) {
			return IndexedTagsPath();
		}
		auto res = impl_->path2indexedtag(jsonPath);
		return res.empty() && canAdd ? impl_.clone()->path2indexedtag(jsonPath, canAdd, updated_) : res;
	}
	int version() const noexcept { return impl_->version(); }
	size_t size() const noexcept { return impl_->size(); }
	bool isUpdated() const noexcept { return updated_; }
	uint32_t stateToken() const noexcept { return impl_->stateToken(); }
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
	void clearUpdated() noexcept { updated_ = false; }
	void setUpdated() noexcept { updated_ = true; }

	bool try_merge(const TagsMatcher& tm) {
		if (impl_->contains(*tm.impl_)) {
			return true;
		}
		auto tmp = impl_;
		bool updated = false;
		if (!tmp.clone()->merge(*tm.impl_, updated)) {
			return false;
		}
		impl_ = tmp;
		updated_ = updated_ || updated;
		return true;
	}
	void add_names_from(const TagsMatcher& tm) {
		auto tmp = impl_;
		if (tmp.clone()->add_names_from(*tm.impl_.get())) {
			updated_ = true;
			impl_ = tmp;
		}
	}

	void UpdatePayloadType(PayloadType payloadType, NeedChangeTmVersion changeVersion) {
		impl_.clone()->updatePayloadType(std::move(payloadType), updated_, changeVersion);
	}
	static TagsMatcher CreateMergedTagsMatcher(const std::vector<TagsMatcher>& tmList) {
		TagsMatcherImpl::TmListT implList;
		implList.reserve(tmList.size());
		for (const auto& tm : tmList) {
			implList.emplace_back(tm.impl_.get());
		}
		TagsMatcher tm(make_intrusive<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>(implList));
		return tm;
	}
	bool IsSubsetOf(const TagsMatcher& otm) const { return impl_->isSubsetOf(*otm.impl_); }

	std::string dump() const { return impl_->dumpTags() + "\n" + impl_->dumpNames() + "\n" + impl_->dumpPaths(); }

private:
	TagsMatcher(intrusive_ptr<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>&& impl) : impl_(std::move(impl)), updated_(false) {}

	shared_cow_ptr<TagsMatcherImpl> impl_;
	bool updated_;
};

}  // namespace reindexer
