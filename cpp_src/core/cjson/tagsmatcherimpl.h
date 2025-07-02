#pragma once

#include <cstdlib>
#include <string>

#include "core/enums.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadtypeimpl.h"
#include "ctag.h"
#include "sparse_index_data.h"
#include "tagspath.h"
#include "tagspathcache.h"
#include "tools/randomgenerator.h"

namespace reindexer {

class Index;
enum class [[nodiscard]] NeedChangeTmVersion { Increment, Decrement, No };

class [[nodiscard]] TagsMatcherImpl {
public:
	using TmListT = h_vector<const TagsMatcherImpl*, 10>;

	TagsMatcherImpl() : version_(0), stateToken_(tools::RandomGenerator::gets32()) {}
	TagsMatcherImpl(PayloadType&& payloadType, std::span<std::unique_ptr<Index>> sparse,
					int32_t stateToken = tools::RandomGenerator::gets32())
		: version_(0), stateToken_(stateToken) {
		WasUpdated wasUpdated = WasUpdated_False;
		UpdatePayloadType(std::move(payloadType), sparse, wasUpdated, NeedChangeTmVersion::No);
	}
	TagsMatcherImpl(const TmListT& tmList) : version_(0), stateToken_(tools::RandomGenerator::gets32()) { createMergedTagsMatcher(tmList); }
	~TagsMatcherImpl() = default;

	TagsPath path2tag(std::string_view jsonPath) const {
		WasUpdated wasUpdated = WasUpdated_False;
		return const_cast<TagsMatcherImpl*>(this)->path2tag(jsonPath, CanAddField_False, wasUpdated);
	}

	TagsPath path2tag(std::string_view jsonPath, CanAddField, WasUpdated&);

	IndexedTagsPath path2indexedtag(std::string_view jsonPath) const {
		WasUpdated wasUpdated = WasUpdated_False;
		return const_cast<TagsMatcherImpl*>(this)->path2indexedtag(jsonPath, CanAddField_False, wasUpdated);
	}

	IndexedTagsPath path2indexedtag(std::string_view jsonPath, CanAddField, WasUpdated&);

	TagName name2tag(std::string_view name) const {
		auto res = names2tags_.find(name);
		return (res == names2tags_.end()) ? TagName::Empty() : res->second;
	}

	TagName name2tag(std::string_view name, CanAddField, WasUpdated&);

	const std::string& tag2name(uint32_t tag) const& { return tag2name(TagName(tag & ctag::kNameMask)); }
	auto tag2name(uint32_t) const&& = delete;
	const std::string& tag2name(TagName tag) const& {
		static const std::string emptystr;
		if (tag.IsEmpty()) {
			return emptystr;
		}
		if (tag.AsNumber() > tags2names_.size()) {
			throw Error(errTagsMissmatch, "Unknown tag {} in cjson", tag.AsNumber());
		}
		return tags2names_[tag.AsNumber() - 1];
	}
	auto tag2name(TagName) const&& = delete;

	FieldProperties tags2field(const std::span<const TagName> path) const noexcept {
		if (path.empty()) {
			return FieldProperties{FieldProperties::kNotIndexed};
		}
		return pathCache_.Lookup(path);
	}
	void BuildTagsCache(WasUpdated&);
	void UpdatePayloadType(PayloadType, std::span<std::unique_ptr<Index>> sparseIndexes, WasUpdated&, NeedChangeTmVersion);
	void UpdatePayloadType(PayloadType, const std::vector<SparseIndexData>& sparseIndexes, WasUpdated&, NeedChangeTmVersion);
	void AddSparseIndex(const Index&);
	void DropSparseIndex(std::string_view name);

	void Serialize(WrSerializer&) const;
	void Deserialize(Serializer&);
	void Deserialize(Serializer&, int version, int stateToken);

	bool AddNamesFrom(const TagsMatcherImpl& tm) {
		bool modified = false;
		for (auto it = tm.names2tags_.begin(), end = tm.names2tags_.end(); it != end; ++it) {
			auto res = names2tags_.emplace(it.key(), TagName(tags2names_.size() + 1));
			if (res.second) {
				tags2names_.emplace_back(it.key());
				++version_;
				modified = true;
			}
		}

		return modified;
	}

	size_t Size() const noexcept { return tags2names_.size(); }
	int Version() const noexcept { return version_; }
	int StateToken() const noexcept { return stateToken_; }

	void Clear() {
		names2tags_.clear();
		tags2names_.clear();
		pathCache_.Clear();
		version_++;
	}
	std::string DumpTags() const;
	std::string DumpNames() const;
	std::string DumpPaths() const;

	std::string Path2Name(const TagsPath& tp) const {
		std::string res;
		for (TagName t : tp) {
			if (!res.empty()) {
				res += '.';
			}
			res += tag2name(t);
		}
		return res;
	}
	// Check if this tagsmatcher includes all the tags from the other tagsmatcher
	bool Contains(const TagsMatcherImpl& tm) const noexcept {
		return tags2names_.size() >= tm.tags2names_.size() && std::equal(tm.tags2names_.begin(), tm.tags2names_.end(), tags2names_.begin());
	}
	// Check if other tagsmatcher includes all the tags from this tagsmatcher
	bool IsSubsetOf(const TagsMatcherImpl& tm) const noexcept { return tm.Contains(*this); }
	bool Merge(const TagsMatcherImpl&, WasUpdated&);
	std::string_view SparseName(size_t n) const& {
		assertrx_throw(n < sparseIndexes_.size());
		return sparseIndexes_[n].name;
	}
	const std::vector<SparseIndexData>& SparseIndexes() const& noexcept { return sparseIndexes_; }
	const SparseIndexData& SparseIndex(size_t n) const& {
		assertrx_throw(n < sparseIndexes_.size());
		return sparseIndexes_[n];
	}

	auto SparseName(size_t) const&& = delete;
	auto SparseIndexes() const&& = delete;
	auto SparseIndex(size_t) const&& = delete;

private:
	void updatePayloadType(PayloadType, WasUpdated&, bool sparseWasUpdated, NeedChangeTmVersion);
	void createMergedTagsMatcher(const TmListT& tmList);
	void validateTagSize(size_t sz) {
		if (sz > ctag::kNameMax) {
			throw Error(errParams, "Exceeded the maximum allowed number ({}) of tags for TagsMatcher. Attempt to place {} tags",
						ctag::kNameMax, sz);
		}
	}
	void addSparseIndex(const Index&);

	fast_hash_map<std::string, TagName, hash_str, equal_str, less_str> names2tags_;
	std::vector<std::string> tags2names_;
	PayloadType payloadType_;
	int32_t version_;
	int32_t stateToken_;
	TagsPathCache pathCache_;
	std::vector<SparseIndexData> sparseIndexes_;
};

}  // namespace reindexer
