#pragma once

#include <algorithm>
#include "core/cjson/tagsmatcher.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "vendor/mpark/variant.h"

namespace reindexer {

static constexpr int maxIndexes = 64;

using base_fields_set = h_vector<int8_t, 6>;
using FieldsPath = mpark::variant<TagsPath, IndexedTagsPath>;

class FieldsSet : protected base_fields_set {
public:
	using base_fields_set::begin;
	using base_fields_set::end;
	using base_fields_set::iterator;
	using base_fields_set::size;
	using base_fields_set::empty;
	using base_fields_set::operator[];
	FieldsSet(const TagsMatcher &tagsMatcher, const h_vector<string, 1> &fields) : mask_(0) {
		for (const string &str : fields) {
			tagsPaths_.emplace_back(tagsMatcher.path2tag(str));
		}
	}
	FieldsSet(std::initializer_list<int> l) : mask_(0) {
		for (auto f : l) push_back(f);
	}
	FieldsSet(std::initializer_list<TagsPath> l) : mask_(0) {
		for (const TagsPath &tagsPath : l) push_back(tagsPath);
	}
	FieldsSet(std::initializer_list<IndexedTagsPath> l) : mask_(0) {
		for (const IndexedTagsPath &tagsPath : l) push_back(tagsPath);
	}
	FieldsSet() = default;

	void push_back(const string &jsonPath) {
		if (!contains(jsonPath)) jsonPaths_.push_back(jsonPath);
	}

	void push_back(const TagsPath &tagsPath) {
		if (!contains(tagsPath)) {
			base_fields_set::push_back(IndexValueType::SetByJsonPath);
			tagsPaths_.emplace_back(tagsPath);
		}
	}

	void push_back(const IndexedTagsPath &tagsPath) {
		if (!contains(tagsPath)) {
			base_fields_set::push_back(IndexValueType::SetByJsonPath);
			tagsPaths_.emplace_back(tagsPath);
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
	bool contains(const string &jsonPath) const { return std::find(jsonPaths_.begin(), jsonPaths_.end(), jsonPath) != jsonPaths_.end(); }
	bool contains(const TagsPath &tagsPath) const {
		for (const FieldsPath &path : tagsPaths_) {
			if (path.index() == 0) {
				if (mpark::get<TagsPath>(path) == tagsPath) return true;
			} else {
				if (mpark::get<IndexedTagsPath>(path).Compare(tagsPath)) return true;
			}
		}
		return false;
	}
	bool contains(const IndexedTagsPath &tagsPath) const {
		for (const FieldsPath &path : tagsPaths_) {
			if (path.index() == 1) {
				if (mpark::get<IndexedTagsPath>(path) == tagsPath) return true;
			} else {
				if (tagsPath.Compare(mpark::get<TagsPath>(path))) return true;
			}
		}
		return false;
	}
	bool match(const TagsPath &tagsPath) const {
		if (tagsPaths_.empty()) return true;
		for (auto &flt : tagsPaths_) {
			if (flt.index() == 0) {
				if (comparePaths(tagsPath, mpark::get<TagsPath>(flt))) return true;
			} else {
				if (comparePaths(mpark::get<IndexedTagsPath>(flt), tagsPath)) return true;
			}
		}
		return false;
	}
	bool match(const IndexedTagsPath &tagsPath) const {
		if (tagsPaths_.empty()) return true;
		for (auto &flt : tagsPaths_) {
			if (flt.index() == 1) {
				if (comparePaths(tagsPath, mpark::get<IndexedTagsPath>(flt))) return true;
			} else {
				if (comparePaths(tagsPath, mpark::get<TagsPath>(flt))) return true;
			}
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
	bool isTagsPathIndexed(size_t idx) const {
		assert(idx < tagsPaths_.size());
		return (tagsPaths_[idx].index() == 1);
	}
	const TagsPath &getTagsPath(size_t idx) const { return mpark::get<TagsPath>(tagsPaths_[idx]); }
	const IndexedTagsPath &getIndexedTagsPath(size_t idx) const { return mpark::get<IndexedTagsPath>(tagsPaths_[idx]); }
	const string &getJsonPath(size_t idx) const { return jsonPaths_[idx]; }

	bool operator==(const FieldsSet &f) const { return (mask_ == f.mask_) && (tagsPaths_ == f.tagsPaths_); }
	bool operator!=(const FieldsSet &f) const { return mask_ != f.mask_ || tagsPaths_ != f.tagsPaths_; }

protected:
	template <typename TPath1, typename TPath2>
	bool comparePaths(const TPath1 &lhs, const TPath2 &rhs) const {
		unsigned i = 0, count = std::min(lhs.size(), rhs.size());
		for (; i < count && lhs[i] == rhs[i]; i++) {
		}
		return (i == count);
	}

	uint64_t mask_ = 0;
	vector<FieldsPath> tagsPaths_;
	/// Json paths to non indexed fields.
	/// Necessary only for composite full text
	/// indexes. There is a connection with
	/// tagsPaths_: order and amount of elements.
	h_vector<string, 1> jsonPaths_;
};

}  // namespace reindexer
