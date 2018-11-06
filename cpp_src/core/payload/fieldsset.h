#pragma once

#include <algorithm>
#include "core/cjson/tagsmatcher.h"
#include "core/query/querywhere.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"

namespace reindexer {

static constexpr int maxIndexes = 64;

using base_fields_set = h_vector<int8_t, 6>;

class FieldsSet : protected base_fields_set {
public:
	using base_fields_set::begin;
	using base_fields_set::end;
	using base_fields_set::iterator;
	using base_fields_set::size;
	using base_fields_set::empty;
	using base_fields_set::operator[];
	FieldsSet(const TagsMatcher &tagsMatcher, const h_vector<string, 4> &fields) : mask_(0) {
		for (auto &str : fields) tagsPaths_.push_back(tagsMatcher.path2tag(str));
	}

	FieldsSet(std::initializer_list<int> l) : mask_(0) {
		for (auto f : l) push_back(f);
	}
	FieldsSet(std::initializer_list<TagsPath> l) : mask_(0) {
		for (const TagsPath &tagsPath : l) push_back(tagsPath);
	}
	FieldsSet() = default;

	void push_back(const string &jsonPath) {
		if (!contains(jsonPath)) jsonPaths_.push_back(jsonPath);
	}

	void push_back(const TagsPath &tagsPath) {
		if (!contains(tagsPath)) {
			base_fields_set::push_back(IndexValueType::SetByJsonPath);
			tagsPaths_.push_back(tagsPath);
		}
	}

	void push_back(int f) {
		if (f == IndexValueType::SetByJsonPath) return;
		assert(f < maxIndexes);
		if (!contains(f)) {
			mask_ |= 1ULL << f;
			base_fields_set::push_back(f);
		}
	}

	void erase(int f) {
		bool byJsonPath = (f == IndexValueType::SetByJsonPath);
		if (byJsonPath || contains(f)) {
			auto it = std::find(begin(), end(), f);
			assert(it != end());
			base_fields_set::erase(it);
			if (!byJsonPath) mask_ &= ~(1ULL << f);
		}
	}

	bool contains(int f) const { return mask_ & (1ULL << f); }
	bool contains(const FieldsSet &f) const { return mask_ && ((mask_ & f.mask_) == mask_); }
	bool contains(const TagsPath &tagsPath) const { return std::find(tagsPaths_.begin(), tagsPaths_.end(), tagsPath) != tagsPaths_.end(); }
	bool contains(const string &jsonPath) const { return std::find(jsonPaths_.begin(), jsonPaths_.end(), jsonPath) != jsonPaths_.end(); }

	bool match(const TagsPath &tagsPath) const {
		if (tagsPaths_.empty()) return true;
		for (auto &flt : tagsPaths_) {
			unsigned i = 0, count = std::min(flt.size(), tagsPath.size());
			for (; i < count && tagsPath[i] == flt[i]; i++) {
			}
			if (i == count) return true;
		}
		return false;
	}

	void clear() {
		base_fields_set::clear();
		tagsPaths_.clear();
		mask_ = 0;
	}
	bool containsAll(int f) const { return (((1 << f) - 1) & mask_) == (1ULL << f) - 1ULL; }

	size_t getTagsPathsLength() const { return tagsPaths_.size(); }
	size_t getJsonPathsLength() const { return jsonPaths_.size(); }
	const TagsPath &getTagsPath(size_t idx) const { return tagsPaths_[idx]; }
	const string &getJsonPath(size_t idx) const { return jsonPaths_[idx]; }

	bool operator==(const FieldsSet &f) const { return (mask_ == f.mask_) && (tagsPaths_ == f.tagsPaths_); }
	bool operator!=(const FieldsSet &f) const { return mask_ != f.mask_ || tagsPaths_ != f.tagsPaths_; }

protected:
	uint64_t mask_ = 0;
	h_vector<TagsPath, 1> tagsPaths_;

	/// Json paths to non indexed fields.
	/// Necessary only for composite full text
	/// indexes. There is a connection with
	/// tagsPaths_: order and amount of elements.
	h_vector<string, 1> jsonPaths_;
};

}  // namespace reindexer
