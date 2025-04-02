#pragma once

#include <algorithm>
#include <bitset>
#include <variant>
#include "core/cjson/tagspath.h"
#include "core/enums.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "estl/overloaded.h"
#include "tools/assertrx.h"

namespace reindexer {

class TagsMatcher;
static constexpr int kMaxIndexes = 256;	 // 'tuple'-index always occupies 1 slot

using base_fields_set = h_vector<int16_t, 6>;
static_assert(std::numeric_limits<base_fields_set::value_type>::max() >= kMaxIndexes,
			  "base_fields_set must be able to store any indexed field number");
static_assert(std::numeric_limits<base_fields_set::value_type>::min() <= SetByJsonPath,
			  "base_fields_set must be able to store non-indexed fields");
static_assert(sizeof(std::bitset<kMaxIndexes>) == 32, "Expecting no overhead from std::bitset");
using FieldsPath = std::variant<TagsPath, IndexedTagsPath>;

using ScalarIndexesSetT = std::bitset<kMaxIndexes>;

class IndexesFieldsSet {
public:
	IndexesFieldsSet() noexcept = default;
	IndexesFieldsSet(int f) { push_back(f); }
	bool contains(int f) const noexcept { return f >= 0 && f < kMaxIndexes && mask_.test(unsigned(f)); }
	void push_back(int f) {
		if (f < 0) {
			return;
		}
		if (f >= kMaxIndexes) {
			throwMaxValueError(f);
		}
		mask_.set(unsigned(f));
	}
	const std::bitset<kMaxIndexes>& mask() const& noexcept { return mask_; }
	const std::bitset<kMaxIndexes>& mask() const&& = delete;
	unsigned count() const noexcept { return mask_.count(); }

private:
	[[noreturn]] void throwMaxValueError(int f);
	std::bitset<kMaxIndexes> mask_;
};

class FieldsSet : protected base_fields_set {
public:
	using base_fields_set::begin;
	using base_fields_set::end;
	using base_fields_set::iterator;
	using base_fields_set::size;
	using base_fields_set::empty;
	using base_fields_set::operator[];
	FieldsSet(const TagsMatcher&, const h_vector<std::string, 1>& fields);
	FieldsSet(int f) { push_back(f); }
	FieldsSet(std::initializer_list<int> l) {
		for (auto f : l) {
			push_back(f);
		}
	}
	FieldsSet(std::initializer_list<TagsPath> l) {
		for (const TagsPath& tagsPath : l) {
			push_back(tagsPath);
		}
	}
	FieldsSet(std::initializer_list<IndexedTagsPath> l) {
		for (const IndexedTagsPath& tagsPath : l) {
			push_back(tagsPath);
		}
	}
	FieldsSet() = default;

	void push_back(const std::string& jsonPath) {
		if (!contains(jsonPath)) {
			jsonPaths_.push_back(jsonPath);
		}
	}
	void push_back(std::string&& jsonPath) {
		if (!contains(jsonPath)) {
			jsonPaths_.emplace_back(std::move(jsonPath));
		}
	}

	void push_back(const TagsPath& tagsPath) { pushBack(tagsPath); }
	void push_back(TagsPath&& tagsPath) { pushBack(std::move(tagsPath)); }
	void push_front(TagsPath&& tagsPath) {
		if (!contains(tagsPath)) {
			base_fields_set::insert(begin(), IndexValueType::SetByJsonPath);
			tagsPaths_.insert(tagsPaths_.begin(), std::move(tagsPath));
		}
	}

	void push_front(const TagsPath& tagsPath) {
		if (!contains(tagsPath)) {
			base_fields_set::insert(begin(), IndexValueType::SetByJsonPath);
			tagsPaths_.emplace(tagsPaths_.begin(), tagsPath);
		}
	}
	void push_back(const IndexedTagsPath& tagsPath) { pushBack(tagsPath); }
	void push_back(IndexedTagsPath&& tagsPath) { pushBack(std::move(tagsPath)); }
	void push_back(const FieldsPath& fieldPath) { pushBack(fieldPath); }
	void push_back(FieldsPath&& fieldPath) { pushBack(std::move(fieldPath)); }

	void push_back(int f) {
		if (f < 0) {
			return;
		}
		if (f >= kMaxIndexes) {
			throwMaxValueError(f);
		}
		if (!contains(f)) {
			mask_.set(unsigned(f));
			base_fields_set::push_back(f);
		}
	}
	void push_front(int f) {
		if (f < 0) {
			return;
		}
		if (f >= kMaxIndexes) {
			throwMaxValueError(f);
		}
		if (!contains(f)) {
			mask_.set(unsigned(f));
			base_fields_set::insert(begin(), f);
		}
	}

	void erase(int f) {
		const bool byJsonPath = (f < 0);
		if (byJsonPath || contains(f)) {
			auto it = std::find(begin(), end(), f);
			assertrx(it != end());
			base_fields_set::erase(it);
			if (!byJsonPath) {
				mask_.reset(unsigned(f));
			}
		}
	}

	bool contains(int f) const noexcept { return f >= 0 && f < kMaxIndexes && mask_.test(unsigned(f)); }
	bool contains(const FieldsSet& f) const noexcept { return (mask_ & f.mask_) == f.mask_; }
	bool contains(std::string_view jsonPath) const noexcept {
		return std::find(jsonPaths_.begin(), jsonPaths_.end(), jsonPath) != jsonPaths_.end();
	}
	bool contains(const IndexesFieldsSet& f) const noexcept { return (mask_ & f.mask()) == f.mask(); }
	bool contains(const TagsPath& tagsPath) const noexcept {
		for (const FieldsPath& path : tagsPaths_) {
			if (std::visit(overloaded{[&tagsPath](const TagsPath& path) { return path == tagsPath; },
									  [&tagsPath](const IndexedTagsPath& path) { return path.Compare(tagsPath); }},
						   path)) {
				return true;
			}
		}
		return false;
	}
	bool contains(const IndexedTagsPath& tagsPath) const noexcept {
		for (const FieldsPath& path : tagsPaths_) {
			if (std::visit(overloaded{[&tagsPath](const TagsPath& path) { return tagsPath.Compare(path); },
									  [&tagsPath](const IndexedTagsPath& path) { return path == tagsPath; }},
						   path)) {
				return true;
			}
		}
		return false;
	}
	bool contains(const FieldsPath& fieldsPath) const noexcept {
		return std::visit([&](const auto& fp) { return contains(fp); }, fieldsPath);
	}
	bool match(const TagsPath& tagsPath) const noexcept {
		if (tagsPaths_.empty()) {
			return true;
		}
		for (auto& path : tagsPaths_) {
			if (std::visit(overloaded{[&tagsPath, this](const TagsPath& path) { return comparePaths(tagsPath, path); },
									  [&tagsPath, this](const IndexedTagsPath& path) { return comparePaths(path, tagsPath); }},
						   path)) {
				return true;
			}
		}
		return false;
	}
	template <unsigned hvSize>
	bool match(const IndexedTagsPathImpl<hvSize>& tagsPath) const noexcept {
		if (tagsPaths_.empty()) {
			return true;
		}
		for (auto& path : tagsPaths_) {
			if (std::visit(overloaded{[&tagsPath, this](const TagsPath& path) { return comparePaths(tagsPath, path); },
									  [&tagsPath, this](const IndexedTagsPath& path) { return comparePaths(tagsPath, path); }},
						   path)) {
				return true;
			}
		}
		return false;
	}
	void clear() noexcept {
		base_fields_set::clear();
		tagsPaths_.clear();
		jsonPaths_.clear();
		mask_.reset();
	}

	size_t getTagsPathsLength() const noexcept { return tagsPaths_.size(); }
	size_t getJsonPathsLength() const noexcept { return jsonPaths_.size(); }
	const h_vector<std::string, 1>& getJsonPaths() const noexcept { return jsonPaths_; }
	bool isTagsPathIndexed(size_t idx) const noexcept {
		assertrx(idx < tagsPaths_.size());
		return std::visit(overloaded{[](const TagsPath&) { return false; }, [](const IndexedTagsPath&) { return true; }}, tagsPaths_[idx]);
	}
	const TagsPath& getTagsPath(size_t idx) const& { return std::get<TagsPath>(tagsPaths_[idx]); }
	const TagsPath& getTagsPath(size_t idx) const&& = delete;
	const IndexedTagsPath& getIndexedTagsPath(size_t idx) const& { return std::get<IndexedTagsPath>(tagsPaths_[idx]); }
	const IndexedTagsPath& getIndexedTagsPath(size_t idx) const&& = delete;
	const FieldsPath& getFieldsPath(size_t idx) const& { return tagsPaths_[idx]; }
	const FieldsPath& getFieldsPath(size_t idx) const&& = delete;
	const std::string& getJsonPath(size_t idx) const& noexcept { return jsonPaths_[idx]; }
	const std::string& getJsonPath(size_t idx) const&& = delete;

	bool operator==(const FieldsSet& f) const noexcept {
		return (mask_ == f.mask_) && (tagsPaths_ == f.tagsPaths_) && (jsonPaths_ == jsonPaths_);
	}
	bool operator!=(const FieldsSet& f) const noexcept { return !(*this == f); }

	template <typename T>
	void Dump(T& os, DumpWithMask withMask) const {
		const DumpFieldsPath fieldsPathDumper{os};
		os << "{[";
		for (auto b = begin(), it = b, e = end(); it != e; ++it) {
			if (it != b) {
				os << ", ";
			}
			os << *it;
		}
		os << "], ";
		if (withMask == DumpWithMask_True) {
			os << "mask: " << mask_ << ", ";
		}
		os << "tagsPaths: [";
		for (auto b = tagsPaths_.cbegin(), it = b, e = tagsPaths_.cend(); it != e; ++it) {
			if (it != b) {
				os << ", ";
			}
			std::visit(fieldsPathDumper, *it);
		}
		os << "]}";
		os << "], jsonPaths: [";
		for (auto b = jsonPaths_.cbegin(), it = b, e = jsonPaths_.cend(); it != e; ++it) {
			if (it != b) {
				os << ", ";
			}
			os << *it;
		}
		os << "]}";
	}
	std::string ToString(DumpWithMask withMask) const;

private:
	template <typename F>
	void pushBack(F&& fieldPath) {
		if (!contains(fieldPath)) {
			base_fields_set::push_back(IndexValueType::SetByJsonPath);
			tagsPaths_.emplace_back(std::forward<F>(fieldPath));
		}
	}
	template <typename TPath1, typename TPath2>
	bool comparePaths(const TPath1& lhs, const TPath2& rhs) const noexcept {
		unsigned i = 0, count = std::min(lhs.size(), rhs.size());
		for (; i < count && lhs[i] == rhs[i]; ++i) {
		}
		return (i == count);
	}
	[[noreturn]] void throwMaxValueError(int f);

	std::bitset<kMaxIndexes> mask_;
	h_vector<FieldsPath, 1> tagsPaths_;
	/// Json paths to non indexed fields.
	/// Necessary only for composite full text
	/// indexes. There is a connection with
	/// tagsPaths_: order and amount of elements.
	h_vector<std::string, 1> jsonPaths_;

	template <typename T>
	class DumpFieldsPath {
	public:
		DumpFieldsPath(T& os) noexcept : os_{os} {}
		void operator()(const TagsPath& tp) const {
			os_ << '[';
			for (auto b = tp.cbegin(), it = b, e = tp.cend(); it != e; ++it) {
				if (it != b) {
					os_ << ", ";
				}
				os_ << it->AsNumber();
			}
			os_ << ']';
		}
		void operator()(const IndexedTagsPath& tp) const {
			os_ << '[';
			for (auto b = tp.cbegin(), it = b, e = tp.cend(); it != e; ++it) {
				if (it != b) {
					os_ << ", ";
				}
				os_ << '?';
			}
			os_ << ']';
		}

	private:
		T& os_;
	};
};

}  // namespace reindexer
