#pragma once

#include <algorithm>
#include <bitset>
#include <variant>
#include "core/cjson/tagspath.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"

namespace reindexer {

class TagsMatcher;
static constexpr int maxIndexes = 64;

using base_fields_set = h_vector<int8_t, 6>;
using FieldsPath = std::variant<TagsPath, IndexedTagsPath>;

class FieldsSet : protected base_fields_set {
public:
	using base_fields_set::begin;
	using base_fields_set::end;
	using base_fields_set::iterator;
	using base_fields_set::size;
	using base_fields_set::empty;
	using base_fields_set::operator[];
	FieldsSet(const TagsMatcher &, const h_vector<std::string, 1> &fields);
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

	void push_back(const std::string &jsonPath) {
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
		assertrx(f < maxIndexes);
		if (!contains(f)) {
			mask_ |= 1ULL << f;
			base_fields_set::push_back(f);
		}
	}

	void erase(int f) {
		bool byJsonPath = (f == IndexValueType::SetByJsonPath);
		if (byJsonPath || contains(f)) {
			auto it = std::find(begin(), end(), f);
			assertrx(it != end());
			base_fields_set::erase(it);
			if (!byJsonPath) mask_ &= ~(1ULL << f);
		}
	}

	bool contains(int f) const noexcept { return mask_ & (1ULL << f); }
	bool contains(const FieldsSet &f) const noexcept { return mask_ && ((mask_ & f.mask_) == f.mask_); }
	bool contains(const std::string &jsonPath) const noexcept {
		return std::find(jsonPaths_.begin(), jsonPaths_.end(), jsonPath) != jsonPaths_.end();
	}
	bool contains(const TagsPath &tagsPath) const noexcept {
		for (const FieldsPath &path : tagsPaths_) {
			if (path.index() == 0) {
				if (std::get<TagsPath>(path) == tagsPath) return true;
			} else {
				if (std::get<IndexedTagsPath>(path).Compare(tagsPath)) return true;
			}
		}
		return false;
	}
	bool contains(const IndexedTagsPath &tagsPath) const noexcept {
		for (const FieldsPath &path : tagsPaths_) {
			if (path.index() == 1) {
				if (std::get<IndexedTagsPath>(path) == tagsPath) return true;
			} else {
				if (tagsPath.Compare(std::get<TagsPath>(path))) return true;
			}
		}
		return false;
	}
	bool match(const TagsPath &tagsPath) const noexcept {
		if (tagsPaths_.empty()) return true;
		for (auto &flt : tagsPaths_) {
			if (flt.index() == 0) {
				if (comparePaths(tagsPath, std::get<TagsPath>(flt))) return true;
			} else {
				if (comparePaths(std::get<IndexedTagsPath>(flt), tagsPath)) return true;
			}
		}
		return false;
	}
	template <unsigned hvSize>
	bool match(const IndexedTagsPathImpl<hvSize> &tagsPath) const noexcept {
		if (tagsPaths_.empty()) return true;
		for (auto &flt : tagsPaths_) {
			if (flt.index() == 1) {
				if (comparePaths(tagsPath, std::get<IndexedTagsPath>(flt))) return true;
			} else {
				if (comparePaths(tagsPath, std::get<TagsPath>(flt))) return true;
			}
		}
		return false;
	}
	void clear() noexcept {
		base_fields_set::clear();
		tagsPaths_.clear();
		jsonPaths_.clear();
		mask_ = 0;
	}

	size_t getTagsPathsLength() const noexcept { return tagsPaths_.size(); }
	size_t getJsonPathsLength() const noexcept { return jsonPaths_.size(); }
	const h_vector<std::string, 1> &getJsonPaths() const noexcept { return jsonPaths_; }
	bool isTagsPathIndexed(size_t idx) const noexcept {
		assertrx(idx < tagsPaths_.size());
		return (tagsPaths_[idx].index() == 1);
	}
	const TagsPath &getTagsPath(size_t idx) const { return std::get<TagsPath>(tagsPaths_[idx]); }
	const IndexedTagsPath &getIndexedTagsPath(size_t idx) const { return std::get<IndexedTagsPath>(tagsPaths_[idx]); }
	const std::string &getJsonPath(size_t idx) const { return jsonPaths_[idx]; }

	bool operator==(const FieldsSet &f) const noexcept {
		return (mask_ == f.mask_) && (tagsPaths_ == f.tagsPaths_) && (jsonPaths_ == jsonPaths_);
	}
	bool operator!=(const FieldsSet &f) const noexcept { return !(*this == f); }

	template <typename T>
	void Dump(T &os) const {
		DumpFieldsPath const fieldsPathDumper{os};
		os << "{[";
		for (auto b = begin(), it = b, e = end(); it != e; ++it) {
			if (it != b) os << ", ";
			os << *it;
		}
		os << "], mask: " << std::bitset<64>{mask_} << ", tagsPaths: [";
		for (auto b = tagsPaths_.cbegin(), it = b, e = tagsPaths_.cend(); it != e; ++it) {
			if (it != b) os << ", ";
			std::visit(fieldsPathDumper, *it);
		}
		os << "]}";
		os << "], jsonPaths: [";
		for (auto b = jsonPaths_.cbegin(), it = b, e = jsonPaths_.cend(); it != e; ++it) {
			if (it != b) os << ", ";
			os << *it;
		}
		os << "]}";
	}

protected:
	template <typename TPath1, typename TPath2>
	bool comparePaths(const TPath1 &lhs, const TPath2 &rhs) const noexcept {
		unsigned i = 0, count = std::min(lhs.size(), rhs.size());
		for (; i < count && lhs[i] == rhs[i]; ++i) {
		}
		return (i == count);
	}

	uint64_t mask_ = 0;
	h_vector<FieldsPath, 1> tagsPaths_;
	/// Json paths to non indexed fields.
	/// Necessary only for composite full text
	/// indexes. There is a connection with
	/// tagsPaths_: order and amount of elements.
	h_vector<std::string, 1> jsonPaths_;

	template <typename T>
	class DumpFieldsPath {
	public:
		DumpFieldsPath(T &os) noexcept : os_{os} {}
		void operator()(const TagsPath &tp) const {
			os_ << '[';
			for (auto b = tp.cbegin(), it = b, e = tp.cend(); it != e; ++it) {
				if (it != b) os_ << ", ";
				os_ << *it;
			}
			os_ << ']';
		}
		void operator()(const IndexedTagsPath &tp) const {
			os_ << '[';
			for (auto b = tp.cbegin(), it = b, e = tp.cend(); it != e; ++it) {
				if (it != b) os_ << ", ";
				os_ << '?';
			}
			os_ << ']';
		}

	private:
		T &os_;
	};
};

}  // namespace reindexer
