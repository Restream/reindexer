#include "tagsmatcherimpl.h"
#include <sstream>
#include "core/index/index.h"
#include "tools/serializer.h"

namespace reindexer {

TagsPath TagsMatcherImpl::path2tag(std::string_view jsonPath, CanAddField canAdd, WasUpdated& wasUpdated) {
	TagsPath fieldTags;
	for (size_t nextPos = 0, lastPos = 0; nextPos != jsonPath.length(); lastPos = nextPos + 1) {
		nextPos = jsonPath.find('.', lastPos);
		if (nextPos == std::string_view::npos) {
			nextPos = jsonPath.length();
		}
		if (nextPos != lastPos) {
			const std::string_view field = jsonPath.substr(lastPos, nextPos - lastPos);
			const TagName fieldTag = name2tag(field, canAdd, wasUpdated);
			if (fieldTag.IsEmpty()) {
				fieldTags.clear();
				return fieldTags;
			}
			fieldTags.emplace_back(fieldTag);
		}
	}
	return fieldTags;
}

IndexedTagsPath TagsMatcherImpl::path2indexedtag(std::string_view jsonPath, CanAddField canAdd, WasUpdated& wasUpdated) {
	using namespace std::string_view_literals;
	IndexedTagsPath fieldTags;
	for (size_t nextPos = 0, lastPos = 0; nextPos != jsonPath.length(); lastPos = nextPos + 1) {
		nextPos = jsonPath.find('.', lastPos);
		if (nextPos == std::string_view::npos) {
			nextPos = jsonPath.length();
		}
		if (nextPos != lastPos) {
			std::string_view indexedField = jsonPath.substr(lastPos, nextPos - lastPos);
			std::string_view fieldName = indexedField;
			auto openBracketPos = indexedField.find('[');
			if (openBracketPos != std::string_view::npos) {
				fieldName = indexedField.substr(0, openBracketPos);
			}
			const TagName tagName = name2tag(fieldName, canAdd, wasUpdated);
			if (tagName.IsEmpty()) {
				fieldTags.clear();
				return fieldTags;
			}
			fieldTags.emplace_back(tagName);
			while (openBracketPos != std::string_view::npos) {
				const size_t closeBracketPos = indexedField.find(']', openBracketPos);
				if (closeBracketPos == std::string_view::npos) {
					throw Error(errParams, "No closing bracket for index in jsonpath");
				}
				const std::string_view content = indexedField.substr(openBracketPos + 1, closeBracketPos - openBracketPos - 1);
				if (content.empty()) {
					throw Error(errParams, "Index value in brackets cannot be empty");
				}
				if (content == "*"sv) {
					fieldTags.emplace_back(TagIndex::All());
				} else {
					auto index = try_stoi(content);
					if (!index) {
						throw Error(errParams, "Can't convert '{}' to number", content);
					}
					if (index < 0) {
						throw Error(errLogic, "Array index value cannot be negative");
					}
					fieldTags.emplace_back(TagIndex{*index});
				}
				indexedField = indexedField.substr(closeBracketPos);
				openBracketPos = indexedField.find('[');
			}
		}
	}
	return fieldTags;
}

TagName TagsMatcherImpl::name2tag(std::string_view n, CanAddField canAdd, WasUpdated& wasUpdated) {
	TagName tag = name2tag(n);
	if (!tag.IsEmpty() || !canAdd) {
		return tag;
	}
	std::string name(n);
	validateTagSize(tags2names_.size() + 1);
	auto res = names2tags_.emplace(name, TagName(tags2names_.size() + 1));
	if (res.second) {
		tags2names_.emplace_back(std::move(name));
		++version_;
	}
	wasUpdated |= res.second;
	return res.first->second;
}

void TagsMatcherImpl::BuildTagsCache(WasUpdated& wasUpdated) {
	pathCache_.Clear();
	if (!payloadType_ && sparseIndexes_.empty()) {
		return;
	}
	std::vector<std::string> pathParts;
	std::vector<TagName> pathIdx;
	for (int i = 1, s = payloadType_->NumFields(); i < s; ++i) {
		const auto& field = payloadType_->Field(i);
		for (const auto& jsonPath : field.JsonPaths()) {
			if (jsonPath.empty()) {
				continue;
			}
			pathIdx.clear();
			for (const auto& name : split(jsonPath, ".", true, pathParts)) {
				pathIdx.emplace_back(name2tag(name, CanAddField_True, wasUpdated));
			}
			pathCache_.Set(pathIdx, FieldProperties{i, field.ArrayDims(), field.IsArray(), field.Type(), IsSparse_False});
		}
	}
	for (int i = 0, s = sparseIndexes_.size(); i < s; ++i) {
		const auto& sparse = sparseIndexes_[i];
		for (const auto& path : sparse.paths) {
			if (path.empty()) {
				continue;
			}
			pathCache_.Set(path, FieldProperties{i, sparse.ArrayDim(), sparse.isArray, sparse.dataType, IsSparse_True});
		}
	}
}

void TagsMatcherImpl::UpdatePayloadType(PayloadType payloadType, const std::vector<SparseIndexData>& sparseIndexes, WasUpdated& wasUpdated,
										NeedChangeTmVersion changeVersion) {
	if (!payloadType && !payloadType_ && sparseIndexes.empty() && sparseIndexes_.empty()) {
		return;
	}
	const bool newSparse = sparseIndexes != sparseIndexes_;
	if (newSparse) {
		sparseIndexes_ = sparseIndexes;
	}
	updatePayloadType(std::move(payloadType), wasUpdated, newSparse, changeVersion);
}

void TagsMatcherImpl::UpdatePayloadType(PayloadType payloadType, std::span<std::unique_ptr<Index>> sparseIndexes, WasUpdated& wasUpdated,
										NeedChangeTmVersion changeVersion) {
	if (!payloadType && !payloadType_ && sparseIndexes.empty() && sparseIndexes_.empty()) {
		return;
	}
	bool newSparse = sparseIndexes_.size() != sparseIndexes.size();
	for (size_t i = 0, s = sparseIndexes_.size(); !newSparse && i < s; ++i) {
		const auto& lhs = sparseIndexes_[i];
		const auto& rhs = *sparseIndexes[i];
		newSparse = !lhs.dataType.IsSame(rhs.KeyType()) || lhs.indexType != rhs.Type() || lhs.isArray != rhs.Opts().IsArray() ||
					lhs.name != rhs.Name() || lhs.paths.size() != rhs.Fields().size();
		for (size_t j = 0, fs = lhs.paths.size(); !newSparse && j < fs; ++j) {
			assertrx(rhs.Fields()[j] == SetByJsonPath);
			newSparse = lhs.paths[j] != rhs.Fields().getTagsPath(j);
		}
	}
	if (newSparse) {
		sparseIndexes_.clear();
		sparseIndexes_.reserve(sparseIndexes.size());
		for (const auto& idx : sparseIndexes) {
			addSparseIndex(*idx);
		}
	}
	updatePayloadType(std::move(payloadType), wasUpdated, newSparse, changeVersion);
}

void TagsMatcherImpl::updatePayloadType(PayloadType payloadType, WasUpdated& wasUpdated, bool sparseWasUpdated,
										NeedChangeTmVersion changeVersion) {
	std::swap(payloadType_, payloadType);
	WasUpdated newType = WasUpdated_False;
	BuildTagsCache(newType);
	newType |= (bool(payloadType) != bool(payloadType_) || (payloadType_.NumFields() != payloadType.NumFields()));
	if (!newType) {
		for (int field = 1, fields = payloadType_.NumFields(); field < fields; ++field) {
			const auto& lf = payloadType_.Field(field);
			const auto& rf = payloadType.Field(field);
			if (!lf.Type().IsSame(rf.Type()) || lf.IsArray() != rf.IsArray() || lf.JsonPaths() != rf.JsonPaths()) {
				newType = WasUpdated_True;
				break;
			}
		}
	}
	wasUpdated |= (newType || sparseWasUpdated);
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

void TagsMatcherImpl::addSparseIndex(const Index& idx) {
	sparseIndexes_.emplace_back(idx.Name(), idx.Type(), idx.KeyType(), idx.Opts().IsArray(), idx.Fields());
}

void TagsMatcherImpl::AddSparseIndex(const Index& idx) {
	addSparseIndex(idx);
	const auto& sparse = sparseIndexes_.back();
	for (const auto& path : sparse.paths) {
		pathCache_.Set(path, FieldProperties(sparseIndexes_.size() - 1, sparse.ArrayDim(), sparse.isArray, sparse.dataType, IsSparse_True));
	}
}

void TagsMatcherImpl::DropSparseIndex(std::string_view name) {
	for (auto it = sparseIndexes_.begin(), end = sparseIndexes_.end(); it != end; ++it) {
		if (it->name == name) {
			sparseIndexes_.erase(it);
			WasUpdated wasUpdated = WasUpdated_False;
			BuildTagsCache(wasUpdated);
			return;
		}
	}
}

void TagsMatcherImpl::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(tags2names_.size());
	for (size_t tag = 0; tag < tags2names_.size(); ++tag) {
		ser.PutVString(tags2names_[tag]);
	}
}

void TagsMatcherImpl::Deserialize(Serializer& ser) {
	Clear();
	const size_t cnt = ser.GetVarUInt();
	validateTagSize(cnt);
	tags2names_.resize(cnt);
	for (size_t tag = 0; tag < tags2names_.size(); ++tag) {
		std::string name(ser.GetVString());
		names2tags_.emplace(name, TagName(tag + 1));
		tags2names_[tag] = name;
	}
}

void TagsMatcherImpl::Deserialize(Serializer& ser, int version, int stateToken) {
	Deserialize(ser);
	version_ = version;
	stateToken_ = stateToken;
}

bool TagsMatcherImpl::Merge(const TagsMatcherImpl& tm, WasUpdated& wasUpdated) {
	if (tm.Contains(*this)) {
		const auto oldSz = Size();
		const auto newSz = tm.names2tags_.size();
		tags2names_.resize(newSz);
		for (size_t i = oldSz; i < newSz; ++i) {
			tags2names_[i] = tm.tags2names_[i];
			const auto r = names2tags_.emplace(tags2names_[i], TagName(i + 1));
			if (!r.second) {
				// unexpected names conflict (this should never happen)
				return false;
			}
		}
		if (oldSz != newSz) {
			wasUpdated = WasUpdated_True;
			if (version_ >= tm.version_) {
				++version_;
			} else {
				version_ = tm.version_;
			}
		}
		return true;
	}
	return Contains(tm);
}

void TagsMatcherImpl::createMergedTagsMatcher(const TmListT& tmList) {
	// Create unique state token
	while (true) {
		const auto found = std::find_if(tmList.begin(), tmList.end(),
										[this](const TagsMatcherImpl* tm) { return tm && tm->StateToken() == StateToken(); });
		if (found != tmList.end()) {
			stateToken_ = tools::RandomGenerator::gets32();
		} else {
			break;
		}
	}

	// Create merged tags list
	for (const auto& tm : tmList) {
		if (!tm) {
			continue;
		}

		for (unsigned tag = 0; tag < tm->tags2names_.size(); ++tag) {
			auto resp = names2tags_.try_emplace(tm->tags2names_[tag], TagName(tags2names_.size() + 1));
			if (resp.second) {	// New tag
				tags2names_.emplace_back(tm->tags2names_[tag]);
			}
		}
	}
}

std::string TagsMatcherImpl::DumpTags() const {
	std::string res = "tags: [";
	for (unsigned i = 0; i < tags2names_.size(); i++) {
		res += std::to_string(i) + ':' + tags2names_[i] + ' ';
	}
	return res + ']';
}

std::string TagsMatcherImpl::DumpNames() const {
	using namespace std::string_view_literals;
	std::stringstream res;
	res << "names: ["sv;
	for (auto b = names2tags_.begin(), it = b, e = names2tags_.end(); it != e; ++it) {
		if (it != b) {
			res << "; "sv;
		}
		res << it->first << ':' << it->second.AsNumber();
	}
	res << ']';
	return res.str();
}

std::string TagsMatcherImpl::DumpPaths() const {
	std::string res = "paths: [";
	std::vector<TagName> path;
	pathCache_.Walk(path, [&path, &res, this](const FieldProperties& field) {
		assertrx(field.IsIndexed());
		for (size_t i = 0; i < path.size(); ++i) {
			if (i) {
				res += '.';
			}
			res += tag2name(path[i]) + '(' + std::to_string(path[i].AsNumber()) + ')';
		}
		res += ':';
		if (field.IsSparse()) {
			res += sparseIndexes_[field.SparseNumber()].name + "(sparse";
		} else {
			res += payloadType_.Field(field.IndexNumber()).Name() + '(' + std::to_string(field.IndexNumber());
		}
		res += (field.IsArray() ? ",array," : ",") + std::string(field.ValueType().Name()) + ") ";
	});
	return res + ']';
}

}  // namespace reindexer
