#pragma once

#include <cstdlib>
#include <string>

#include "core/keyvalue/key_string.h"
#include "core/payload/payloadtype.h"
#include "ctag.h"
#include "tagspathcache.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

using std::string;
using TagsPath = h_vector<int16_t, 6>;

struct IndexedPathNode {
	IndexedPathNode() = default;
	IndexedPathNode(int16_t _nameTag) : nameTag_(_nameTag) {}
	IndexedPathNode(int16_t _nameTag, int32_t _index) : nameTag_(_nameTag), index_(_index) {}
	bool operator==(const IndexedPathNode &obj) const noexcept {
		if (nameTag_ != obj.nameTag_) return false;
		if (IsForAllItems() || obj.IsForAllItems()) return true;
		if (index_ != IndexValueType::NotSet && obj.index_ != IndexValueType::NotSet) {
			if (index_ != obj.index_) return false;
		}
		return true;
	}
	bool operator!=(const IndexedPathNode &obj) const noexcept { return !(operator==(obj)); }
	bool operator==(int16_t _nameTag) const noexcept { return _nameTag == nameTag_; }
	bool operator!=(int16_t _nameTag) const noexcept { return _nameTag != nameTag_; }
	explicit operator int() const noexcept { return nameTag_; }

	int NameTag() const noexcept { return nameTag_; }
	int Index() const noexcept { return index_; }
	string_view Expression() const {
		if (expression_ && expression_->length() > 0) {
			return string_view(expression_->c_str(), expression_->length());
		}
		return string_view();
	}

	bool IsArrayNode() const noexcept { return (IsForAllItems() || index_ != IndexValueType::NotSet); }
	bool IsWithIndex() const noexcept { return index_ != ForAllItems && index_ != IndexValueType::NotSet; }
	bool IsWithExpression() const { return expression_ && !expression_->empty(); }
	bool IsForAllItems() const { return index_ == ForAllItems; }

	void MarkAllItems(bool enable) {
		if (enable) {
			index_ = ForAllItems;
		} else if (index_ == ForAllItems) {
			index_ = IndexValueType::NotSet;
		}
	}

	void SetExpression(string_view v) {
		if (expression_) {
			expression_->assign(v.data(), v.length());
		} else {
			expression_ = make_key_string(v.data(), v.length());
		}
	}

	void SetIndex(int32_t index) { index_ = index; }
	void SetNameTag(int16_t nameTag) { nameTag_ = nameTag; }

private:
	enum : int32_t { ForAllItems = -2 };
	int16_t nameTag_ = 0;
	int32_t index_ = IndexValueType::NotSet;
	key_string expression_;
};

class IndexedTagsPath : public h_vector<IndexedPathNode, 6> {
public:
	using Base = h_vector<IndexedPathNode, 6>;
	using Base::Base;
	bool Compare(const IndexedTagsPath &obj) const {
		const size_t ourSize = size();
		if (obj.size() != ourSize) return false;
		if (back().IsArrayNode() != obj.back().IsArrayNode()) return false;
		for (size_t i = 0; i < ourSize; ++i) {
			const IndexedPathNode &ourNode = operator[](i);
			if (i == ourSize - 1) {
				if (ourNode.IsArrayNode()) {
					if (ourNode.NameTag() != obj[i].NameTag()) return false;
					if (ourNode.IsForAllItems() || obj[i].IsForAllItems()) break;
					return (ourNode.Index() == obj[i].Index());
				} else {
					return (ourNode.NameTag() == obj[i].NameTag());
				}
			} else {
				if (ourNode != obj[i]) return false;
			}
		}
		return true;
	};
	bool Compare(const TagsPath &obj) const {
		if (obj.size() != size()) return false;
		for (size_t i = 0; i < size(); ++i) {
			if (operator[](i).NameTag() != obj[i]) return false;
		}
		return true;
	}
};

using IndexExpressionEvaluator = std::function<VariantArray(string_view)>;

template <typename TagsPath>
class TagsPathScope {
public:
	template <typename Node>
	TagsPathScope(TagsPath &tagsPath, Node &&node) : tagsPath_(tagsPath), tagName_(static_cast<int>(node)) {
		if (tagName_) tagsPath_.emplace_back(std::move(node));
	}
	~TagsPathScope() {
		if (tagName_ && !tagsPath_.empty()) tagsPath_.pop_back();
	}
	TagsPathScope(const TagsPathScope &) = delete;
	TagsPathScope &operator=(const TagsPathScope &) = delete;

private:
	TagsPath &tagsPath_;
	int tagName_;
};

class TagsMatcherImpl {
public:
	TagsMatcherImpl() : version_(0), stateToken_(rand()) {}
	TagsMatcherImpl(PayloadType payloadType) : payloadType_(payloadType), version_(0), stateToken_(rand()) {}
	~TagsMatcherImpl() {}

	TagsPath path2tag(string_view jsonPath) const {
		bool updated = false;
		return const_cast<TagsMatcherImpl *>(this)->path2tag(jsonPath, false, updated);
	}

	TagsPath path2tag(string_view jsonPath, bool canAdd, bool &updated) {
		TagsPath fieldTags;
		for (size_t pos = 0, lastPos = 0; pos != jsonPath.length(); lastPos = pos + 1) {
			pos = jsonPath.find('.', lastPos);
			if (pos == string_view::npos) {
				pos = jsonPath.length();
			}
			if (pos != lastPos) {
				string_view field = jsonPath.substr(lastPos, pos - lastPos);
				int fieldTag = name2tag(field, canAdd, updated);
				if (!fieldTag) {
					fieldTags.clear();
					return fieldTags;
				}
				fieldTags.push_back(static_cast<int16_t>(fieldTag));
			}
		}
		return fieldTags;
	}

	IndexedTagsPath path2indexedtag(string_view jsonPath, IndexExpressionEvaluator ev) const {
		bool updated = false;
		return const_cast<TagsMatcherImpl *>(this)->path2indexedtag(jsonPath, ev, false, updated);
	}

	IndexedTagsPath path2indexedtag(string_view jsonPath, IndexExpressionEvaluator ev, bool canAdd, bool &updated) {
		IndexedTagsPath fieldTags;
		for (size_t pos = 0, lastPos = 0; pos != jsonPath.length(); lastPos = pos + 1) {
			pos = jsonPath.find('.', lastPos);
			if (pos == string_view::npos) {
				pos = jsonPath.length();
			}
			if (pos != lastPos) {
				IndexedPathNode node;
				string_view field = jsonPath.substr(lastPos, pos - lastPos);
				size_t openBracketPos = field.find('[');
				if (openBracketPos != string_view::npos) {
					size_t closeBracketPos = field.find(']', openBracketPos);
					if (closeBracketPos == string_view::npos) {
						throw Error(errParams, "No closing bracket for index in jsonpath");
					}
					string_view content = field.substr(openBracketPos + 1, closeBracketPos - openBracketPos - 1);
					if (content.empty()) {
						throw Error(errParams, "Index value in brackets cannot be empty");
					}
					if (content == "*"_sv) {
						node.MarkAllItems(true);
					} else {
						int index = stoi(content);
						if (index == 0 && content != "0" && ev) {
							VariantArray values = ev(content);
							if (values.size() != 1) {
								throw Error(errParams, "Index expression_ has wrong syntax: '%s'", content);
							}
							if (values.front().Type() != KeyValueDouble && values.front().Type() != KeyValueInt &&
								values.front().Type() != KeyValueInt64) {
								throw Error(errParams, "Wrong type of index: '%s'", content);
							}
							node.SetExpression(content);
							index = values.front().As<int>();
						}
						if (index < 0) {
							throw Error(errLogic, "Array index value cannot be negative");
						}
						node.SetIndex(index);
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

	int name2tag(string_view name) const {
		auto res = names2tags_.find(name);
		return (res == names2tags_.end()) ? 0 : res->second + 1;
	}

	int name2tag(string_view n, bool canAdd, bool &updated) {
		int tag = name2tag(n);
		if (tag || !canAdd) return tag;

		string name(n);
		auto res = names2tags_.emplace(name, tags2names_.size());
		if (res.second) {
			tags2names_.emplace_back(std::move(name));
			version_++;
		}
		updated |= res.second;
		return res.first->second + 1;
	}

	const string &tag2name(int tag) const {
		tag &= (1 << ctag::nameBits) - 1;
		static string emptystr;
		if (tag == 0) return emptystr;

		if (tag - 1 >= int(tags2names_.size())) {
			throw Error(errTagsMissmatch, "Unknown tag %d in cjson", tag);
		}

		return tags2names_[tag - 1];
	}

	int tags2field(const int16_t *path, size_t pathLen) const {
		if (!pathLen) return -1;
		return pathCache_.lookup(path, pathLen);
	}
	void buildTagsCache(bool &updated) {
		if (!payloadType_) return;
		pathCache_.clear();
		vector<string> pathParts;
		vector<int16_t> pathIdx;
		for (int i = 1; i < payloadType_->NumFields(); i++) {
			for (auto &jsonPath : payloadType_->Field(i).JsonPaths()) {
				if (!jsonPath.length()) continue;
				pathIdx.clear();
				for (auto &name : split(jsonPath, ".", true, pathParts)) {
					pathIdx.push_back(name2tag(name, true, updated));
				}
				pathCache_.set(pathIdx.data(), pathIdx.size(), i);
			}
		}
	}
	void updatePayloadType(PayloadType payloadType, bool &updated, bool incVersion) {
		updated = true;
		payloadType_ = payloadType;
		if (incVersion) version_++;
		buildTagsCache(updated);
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
			string name(ser.GetVString());
			names2tags_.emplace(name, tag);
			tags2names_[tag] = name;
		}
		version_++;
		// assert(ser.Eof());
	}
	void deserialize(Serializer &ser, int version, int stateToken) {
		deserialize(ser);
		version_ = version;
		stateToken_ = stateToken;
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

		version_ = std::max(version_, tm.version_) + 1;

		return true;
	}

	size_t size() const { return tags2names_.size(); }
	int version() const { return version_; }
	int stateToken() const { return stateToken_; }

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
	fast_hash_map<string, int, hash_str, equal_str> names2tags_;
	vector<string> tags2names_;
	PayloadType payloadType_;
	int32_t version_;
	int32_t stateToken_;
	TagsPathCache pathCache_;
};
}  // namespace reindexer

namespace std {
template <>
struct hash<reindexer::TagsPath> {
public:
	size_t operator()(const reindexer::TagsPath &v) const {
		return reindexer::_Hash_bytes(v.data(), v.size() * sizeof(typename reindexer::TagsPath::value_type));
	}
};
}  // namespace std
