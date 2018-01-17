#pragma once

#include <string>

#include "core/payload/payloadtype.h"
#include "ctag.h"
#include "tagspathcache.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

using std::string;

class TagsMatcherImpl {
public:
	TagsMatcherImpl(PayloadType::Ptr payloadType = nullptr) : payloadType_(payloadType), version_(0) {}
	~TagsMatcherImpl() {
		//	if (tags2names_.size()) printf("~TagsMatcherImpl::TagsMatcherImpl %d\n", int(tags2names_.size()));
	}

	int name2tag(const char *name) const {
		auto res = names2tags_.find(name);
		return (res == names2tags_.end()) ? 0 : res->second + 1;
	}

	int name2tag(const string &name, const string &path, bool &updated) {
		auto res = names2tags_.emplace(name, tags2names_.size());
		if (res.second) {
			tags2names_.push_back(name);
			version_++;
		}
		updated |= res.second;
		int tag = res.first->second | ((payloadType_->FieldByJsonPath(path) + 1) << ctag::nameBits);
		return tag + 1;
	}

	int name2tag(const char *name, bool canAdd, bool &updated) {
		int tag = name2tag(name);
		if (tag || !canAdd) return tag;

		auto res = names2tags_.emplace(name, tags2names_.size());
		if (res.second) {
			tags2names_.push_back(name);
			version_++;
		}
		updated |= res.second;
		return res.first->second + 1;
	}

	const string &tag2name(int tag) const {
		tag &= (1 << ctag::nameBits) - 1;
		static string emptystr;
		if (tag == 0 || tag - 1 >= int(tags2names_.size())) return emptystr;
		return tags2names_[tag - 1];
	}

	int tags2field(const int *path, size_t pathLen) const {
		if (!pathLen) return -1;
		return pathCache_.lookup(path, pathLen);
	}
	void buildTagsCache(bool &updated) {
		pathCache_.clear();
		vector<string> pathParts;
		vector<int> pathIdx;
		for (int i = 1; i < payloadType_->NumFields(); i++) {
			for (auto &jsonPath : payloadType_->Field(i).JsonPaths()) {
				if (!jsonPath.length()) continue;
				pathIdx.clear();
				for (auto &name : split(jsonPath, ".", true, pathParts)) {
					pathIdx.push_back(name2tag(name.c_str(), true, updated));
				}
				pathCache_.set(pathIdx.data(), pathIdx.size(), i);
			}
		}
	}

	void serialize(WrSerializer &ser) const {
		ser.PutVarUint(tags2names_.size());
		for (size_t tag = 0; tag < tags2names_.size(); ++tag) ser.PutVString(tags2names_[tag]);
	}

	void deserialize(Serializer &ser) {
		clear();
		size_t cnt = ser.GetVarUint();
		tags2names_.resize(cnt);
		for (size_t tag = 0; tag < tags2names_.size(); ++tag) {
			string name = ser.GetVString().ToString();
			names2tags_.emplace(name, tag);
			tags2names_[tag] = name;
		}
		version_++;
	}

	bool merge(const TagsMatcherImpl &tm) {
		auto sz = tm.names2tags_.size();
		auto oldSz = size();

		if (tags2names_.size() < sz) tags2names_.resize(sz);

		auto it = tm.names2tags_.begin();
		auto end = tm.names2tags_.end();
		for (; it != end; ++it) {
			auto r = names2tags_.emplace(it->first, it->second);
			if (!r.second && r.first->second != it->second) {
				// name conflict
				return false;
			}
			if (r.second && it->second < int(oldSz)) {
				// tag conflict
				return false;
			}

			tags2names_[it->second] = it->first;
		}

		version_ = std::max(version_, tm.version_);

		return true;
	}

	size_t size() const { return tags2names_.size(); }
	int version() const { return version_; }

	void clear() {
		names2tags_.clear();
		tags2names_.clear();
		pathCache_.clear();
		version_++;
	}
	string dumpTags() const {
		string res = "tags: [";
		for (unsigned i = 0; i < tags2names_.size(); i++) {
			res += std::to_string(i) + ":" + tags2names_[i] + " ";
		}
		return res + "]";
	}
	string dumpPaths() const {
		string res = "paths: [";
		int path[256];
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
	struct equal_str {
		using is_transparent = void;
		bool operator()(const char *lhs, const string &rhs) const { return !strcmp(lhs, rhs.c_str()); }
		bool operator()(const string &lhs, const char *rhs) const { return !strcmp(lhs.c_str(), rhs); }
		bool operator()(const string &lhs, const string &rhs) const { return rhs == lhs; }
	};

	struct hash_str {
		using is_transparent = void;
		size_t operator()(const char *hs) const { return _Hash_bytes(hs, strlen(hs)); }
		size_t operator()(const string &hs) const { return _Hash_bytes(hs.data(), hs.length()); }
	};

	fast_hash_map<string, int, hash_str, equal_str> names2tags_;
	vector<string> tags2names_;
	PayloadType::Ptr payloadType_;
	int version_;

	TagsPathCache pathCache_;
};
}  // namespace reindexer
