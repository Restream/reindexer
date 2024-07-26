#pragma once

#include <cstdlib>
#include <sstream>
#include <string>

#include "core/payload/payloadtype.h"
#include "core/payload/payloadtypeimpl.h"
#include "ctag.h"
#include "tagspath.h"
#include "tagspathcache.h"
#include "tools/randomgenerator.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

enum class NeedChangeTmVersion { Increment, Decrement, No };

class TagsMatcherImpl {
public:
	using TmListT = h_vector<const TagsMatcherImpl *, 10>;

	TagsMatcherImpl() : version_(0), stateToken_(tools::RandomGenerator::gets32()) {}
	TagsMatcherImpl(PayloadType &&payloadType, int32_t stateToken = tools::RandomGenerator::gets32())
		: version_(0), stateToken_(stateToken) {
		bool updated = false;
		updatePayloadType(std::move(payloadType), updated, NeedChangeTmVersion::No);
		(void)updated;	// No update check required
	}
	TagsMatcherImpl(const TmListT &tmList) : version_(0), stateToken_(tools::RandomGenerator::gets32()) { createMergedTagsMatcher(tmList); }
	~TagsMatcherImpl() = default;

	TagsPath path2tag(std::string_view jsonPath) const {
		bool updated = false;
		return const_cast<TagsMatcherImpl *>(this)->path2tag(jsonPath, false, updated);
	}

	TagsPath path2tag(std::string_view jsonPath, bool canAdd, bool &updated) {
		TagsPath fieldTags;
		for (size_t pos = 0, lastPos = 0; pos != jsonPath.length(); lastPos = pos + 1) {
			pos = jsonPath.find('.', lastPos);
			if (pos == std::string_view::npos) {
				pos = jsonPath.length();
			}
			if (pos != lastPos) {
				std::string_view field = jsonPath.substr(lastPos, pos - lastPos);
				int fieldTag = name2tag(field, canAdd, updated);
				if (!fieldTag) {
					fieldTags.clear();
					return fieldTags;
				}
				fieldTags.emplace_back(static_cast<int16_t>(fieldTag));
			}
		}
		return fieldTags;
	}

	IndexedTagsPath path2indexedtag(std::string_view jsonPath) const {
		bool updated = false;
		return const_cast<TagsMatcherImpl *>(this)->path2indexedtag(jsonPath, false, updated);
	}

	IndexedTagsPath path2indexedtag(std::string_view jsonPath, bool canAdd, bool &updated) {
		using namespace std::string_view_literals;
		IndexedTagsPath fieldTags;
		for (size_t pos = 0, lastPos = 0; pos != jsonPath.length(); lastPos = pos + 1) {
			pos = jsonPath.find('.', lastPos);
			if (pos == std::string_view::npos) {
				pos = jsonPath.length();
			}
			if (pos != lastPos) {
				IndexedPathNode node;
				std::string_view field = jsonPath.substr(lastPos, pos - lastPos);
				size_t openBracketPos = field.find('[');
				if (openBracketPos != std::string_view::npos) {
					size_t closeBracketPos = field.find(']', openBracketPos);
					if (closeBracketPos == std::string_view::npos) {
						throw Error(errParams, "No closing bracket for index in jsonpath");
					}
					std::string_view content = field.substr(openBracketPos + 1, closeBracketPos - openBracketPos - 1);
					if (content.empty()) {
						throw Error(errParams, "Index value in brackets cannot be empty");
					}
					if (content == "*"sv) {
						node.MarkAllItems(true);
					} else {
						auto index = try_stoi(content);
						if (!index) {
							throw Error(errParams, "Can't convert '%s' to number", content);
						}
						if (index < 0) {
							throw Error(errLogic, "Array index value cannot be negative");
						}
						node.SetIndex(*index);
					}
					field = field.substr(0, openBracketPos);
				}
				node.SetNameTag(name2tag(field, canAdd, updated));
				if (!node.NameTag()) {
					fieldTags.clear();
					return fieldTags;
				}
				fieldTags.emplace_back(std::move(node));
			}
		}
		return fieldTags;
	}

	int name2tag(std::string_view name) const {
		auto res = names2tags_.find(name);
		return (res == names2tags_.end()) ? 0 : res->second + 1;
	}

	int name2tag(std::string_view n, bool canAdd, bool &updated) {
		int tag = name2tag(n);
		if (tag || !canAdd) return tag;

		std::string name(n);
		validateTagSize(tags2names_.size() + 1);
		auto res = names2tags_.emplace(name, tags2names_.size());
		if (res.second) {
			tags2names_.emplace_back(std::move(name));
			version_++;
		}
		updated |= res.second;
		return res.first->second + 1;
	}

	const std::string &tag2name(uint32_t tag) const {
		static std::string emptystr;
		tag &= ctag::kNameMask;
		if (tag == 0) return emptystr;

		if (tag - 1 >= tags2names_.size()) {
			throw Error(errTagsMissmatch, "Unknown tag %d in cjson", tag);
		}

		return tags2names_[tag - 1];
	}

	int tags2field(const int16_t *path, size_t pathLen) const noexcept {
		if (!pathLen) return -1;
		return pathCache_.lookup(path, pathLen);
	}
	void buildTagsCache(bool &updated) {
		if (!payloadType_) return;
		pathCache_.clear();
		std::vector<std::string> pathParts;
		std::vector<int16_t> pathIdx;
		for (int i = 1; i < payloadType_->NumFields(); i++) {
			for (auto &jsonPath : payloadType_->Field(i).JsonPaths()) {
				if (!jsonPath.length()) continue;
				pathIdx.clear();
				for (auto &name : split(jsonPath, ".", true, pathParts)) {
					pathIdx.emplace_back(name2tag(name, true, updated));
				}
				pathCache_.set(pathIdx.data(), pathIdx.size(), i);
			}
		}
	}
	void updatePayloadType(PayloadType payloadType, bool &updated, NeedChangeTmVersion changeVersion) {
		if (!payloadType && !payloadType_) {
			return;
		}
		std::swap(payloadType_, payloadType);
		bool newType = false;
		buildTagsCache(newType);
		newType = newType || bool(payloadType) != bool(payloadType_) || (payloadType_.NumFields() != payloadType.NumFields());
		if (!newType) {
			for (int field = 1, fields = payloadType_.NumFields(); field < fields; ++field) {
				auto &lf = payloadType_.Field(field);
				auto &rf = payloadType.Field(field);
				if (!lf.Type().IsSame(rf.Type()) || lf.IsArray() != rf.IsArray() || lf.JsonPaths() != rf.JsonPaths()) {
					newType = true;
					break;
				}
			}
		}
		updated = updated || newType;
		switch (changeVersion) {
			case NeedChangeTmVersion::Increment:
				++version_;
				break;
			case NeedChangeTmVersion::Decrement:
				--version_;
				break;
			case NeedChangeTmVersion::No:
				break;
		}
	}

	void serialize(WrSerializer &ser) const {
		ser.PutVarUint(tags2names_.size());
		for (size_t tag = 0; tag < tags2names_.size(); ++tag) ser.PutVString(tags2names_[tag]);
	}

	void deserialize(Serializer &ser) {
		clear();
		size_t cnt = ser.GetVarUint();
		validateTagSize(cnt);
		tags2names_.resize(cnt);
		for (size_t tag = 0; tag < tags2names_.size(); ++tag) {
			std::string name(ser.GetVString());
			names2tags_.emplace(name, tag);
			tags2names_[tag] = name;
		}
		// assert(ser.Eof());
	}
	void deserialize(Serializer &ser, int version, int stateToken) {
		deserialize(ser);
		version_ = version;
		stateToken_ = stateToken;
	}

	bool merge(const TagsMatcherImpl &tm, bool &updated) {
		if (tm.contains(*this)) {
			auto oldSz = size();
			auto newSz = tm.names2tags_.size();
			tags2names_.resize(newSz);
			for (size_t i = oldSz; i < newSz; ++i) {
				tags2names_[i] = tm.tags2names_[i];
				const auto r = names2tags_.emplace(tags2names_[i], i);
				if (!r.second) {
					// unexpected names conflict (this should never happen)
					return false;
				}
			}
			if (oldSz != newSz) {
				updated = true;
				if (version_ >= tm.version_) {
					++version_;
				} else {
					version_ = tm.version_;
				}
			}
			return true;
		}
		return contains(tm);
	}
	// Check if this tagsmatcher includes all of the tags from the other tagsmatcher
	bool contains(const TagsMatcherImpl &tm) const noexcept {
		return tags2names_.size() >= tm.tags2names_.size() && std::equal(tm.tags2names_.begin(), tm.tags2names_.end(), tags2names_.begin());
	}
	// Check if other tagsmatcher includes all of the tags from this tagsmatcher
	bool isSubsetOf(const TagsMatcherImpl &tm) const noexcept { return tm.contains(*this); }
	bool add_names_from(const TagsMatcherImpl &tm) {
		bool modified = false;
		for (auto it = tm.names2tags_.begin(), end = tm.names2tags_.end(); it != end; ++it) {
			auto res = names2tags_.emplace(it.key(), tags2names_.size());
			if (res.second) {
				tags2names_.emplace_back(it.key());
				++version_;
				modified = true;
			}
		}

		return modified;
	}

	size_t size() const noexcept { return tags2names_.size(); }
	int version() const noexcept { return version_; }
	int stateToken() const noexcept { return stateToken_; }

	void clear() {
		names2tags_.clear();
		tags2names_.clear();
		pathCache_.clear();
		version_++;
	}
	std::string dumpTags() const {
		std::string res = "tags: [";
		for (unsigned i = 0; i < tags2names_.size(); i++) {
			res += std::to_string(i) + ":" + tags2names_[i] + " ";
		}
		return res + "]";
	}
	std::string dumpNames() const {
		std::stringstream res;
		res << "names: [";
		for (auto b = names2tags_.begin(), it = b, e = names2tags_.end(); it != e; ++it) {
			if (it != b) {
				res << "; ";
			}
			res << it->first << ':' << it->second;
		}
		res << ']';
		return res.str();
	}
	std::string dumpPaths() const {
		std::string res = "paths: [";
		int16_t path[256];
		pathCache_.walk(path, 0, [&path, &res, this](int depth, int field) {
			for (int i = 0; i < depth; i++) {
				if (i) res += ".";
				res += tag2name(path[i]) + "(" + std::to_string(path[i]) + ")";
			}
			res += ":" + payloadType_->Field(field).Name() + "(" + std::to_string(field) + ") ";
		});

		return res + "]";
	}

protected:
	void createMergedTagsMatcher(const TmListT &tmList) {
		// Create unique state token
		auto found = tmList.end();
		do {
			found = std::find_if(tmList.begin(), tmList.end(),
								 [this](const TagsMatcherImpl *tm) { return tm && tm->stateToken() == stateToken(); });
			if (found != tmList.end()) {
				stateToken_ = tools::RandomGenerator::gets32();
			}
		} while (found != tmList.end());

		// Create merged tags list
		for (const auto &tm : tmList) {
			if (!tm) continue;

			for (unsigned tag = 0; tag < tm->tags2names_.size(); ++tag) {
				auto resp = names2tags_.try_emplace(tm->tags2names_[tag], tags2names_.size());
				if (resp.second) {	// New tag
					tags2names_.emplace_back(tm->tags2names_[tag]);
				}
			}
		}
	}

	void validateTagSize(size_t sz) {
		if (sz > ctag::kNameMax) {
			throw Error(errParams, "Exceeded the maximum allowed number (%d) of tags for TagsMatcher. Attempt to place %d tags",
						ctag::kNameMax, sz);
		}
	}

	fast_hash_map<std::string, int, hash_str, equal_str, less_str> names2tags_;
	std::vector<std::string> tags2names_;
	PayloadType payloadType_;
	int32_t version_;
	int32_t stateToken_;
	TagsPathCache pathCache_;
};
}  // namespace reindexer
