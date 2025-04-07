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

	TagsMatcher() : impl_(make_intrusive<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>()), wasUpdated_(false) {}
	TagsMatcher(unsafe_empty_t) noexcept : wasUpdated_(false) {}
	TagsMatcher(PayloadType payloadType, int32_t stateToken = tools::RandomGenerator::gets32())
		: impl_(make_intrusive<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>(std::move(payloadType), stateToken)), wasUpdated_(false) {}

	TagName name2tag(std::string_view name) const { return impl_->name2tag(name); }
	TagName name2tag(std::string_view name, CanAddField canAdd) {
		if (!name.data()) {
			return TagName::Empty();  // -V547
		}
		const TagName res = impl_->name2tag(name);
		return res.IsEmpty() ? impl_.clone()->name2tag(name, canAdd, wasUpdated_) : res;
	}
	int tags2field(const std::span<const TagName> path) const noexcept { return impl_->tags2field(path); }
	const std::string& tag2name(TagName tag) const { return impl_->tag2name(tag); }
	TagsPath path2tag(std::string_view jsonPath) const { return impl_->path2tag(jsonPath); }
	TagsPath path2tag(std::string_view jsonPath, CanAddField canAdd) {
		if (jsonPath.empty()) {
			return TagsPath();
		}
		auto res = path2tag(jsonPath);
		return res.empty() && canAdd ? impl_.clone()->path2tag(jsonPath, canAdd, wasUpdated_) : res;
	}
	IndexedTagsPath path2indexedtag(std::string_view jsonPath) const {
		IndexedTagsPath tagsPath = impl_->path2indexedtag(jsonPath);
		assertrx(!wasUpdated_);
		return tagsPath;
	}
	IndexedTagsPath path2indexedtag(std::string_view jsonPath, CanAddField canAdd) {
		if (jsonPath.empty()) {
			return IndexedTagsPath();
		}
		auto res = impl_->path2indexedtag(jsonPath);
		return res.empty() && canAdd ? impl_.clone()->path2indexedtag(jsonPath, canAdd, wasUpdated_) : res;
	}
	int version() const noexcept { return impl_->Version(); }
	size_t size() const noexcept { return impl_->Size(); }
	bool isUpdated() const noexcept { return *wasUpdated_; }
	uint32_t stateToken() const noexcept { return impl_->StateToken(); }
	void clear() { impl_.clone()->Clear(); }
	void serialize(WrSerializer& ser) const { impl_->Serialize(ser); }
	void deserialize(Serializer& ser) {
		impl_.clone()->Deserialize(ser);
		impl_.clone()->BuildTagsCache(wasUpdated_);
	}
	void deserialize(Serializer& ser, int version, int stateToken) {
		impl_.clone()->Deserialize(ser, version, stateToken);
		impl_.clone()->BuildTagsCache(wasUpdated_);
	}
	void clearUpdated() noexcept { wasUpdated_ = WasUpdated_False; }
	void setUpdated() noexcept { wasUpdated_ = WasUpdated_True; }

	bool try_merge(const TagsMatcher& tm) {
		if (impl_->Contains(*tm.impl_)) {
			return true;
		}
		auto tmp = impl_;
		WasUpdated updated = WasUpdated_False;
		if (!tmp.clone()->Merge(*tm.impl_, updated)) {
			return false;
		}
		impl_ = tmp;
		wasUpdated_ |= updated;
		return true;
	}
	void add_names_from(const TagsMatcher& tm) {
		auto tmp = impl_;
		if (tmp.clone()->add_names_from(*tm.impl_.get())) {
			wasUpdated_ = WasUpdated_True;
			impl_ = tmp;
		}
	}

	void UpdatePayloadType(PayloadType payloadType, NeedChangeTmVersion changeVersion) {
		impl_.clone()->UpdatePayloadType(std::move(payloadType), wasUpdated_, changeVersion);
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
	bool IsSubsetOf(const TagsMatcher& otm) const { return impl_->IsSubsetOf(*otm.impl_); }

	std::string Dump() const { return impl_->DumpTags() + "\n" + impl_->DumpNames() + "\n" + impl_->DumpPaths(); }

private:
	TagsMatcher(intrusive_ptr<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>&& impl) : impl_(std::move(impl)), wasUpdated_(false) {}

	shared_cow_ptr<TagsMatcherImpl> impl_;
	WasUpdated wasUpdated_;
};

}  // namespace reindexer
