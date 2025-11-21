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

using base_fields_set = h_vector<int16_t, 6>;
static_assert(std::numeric_limits<base_fields_set::value_type>::max() >= kMaxIndexes,
			  "base_fields_set must be able to store any indexed field number");
static_assert(std::numeric_limits<base_fields_set::value_type>::min() <= SetByJsonPath,
			  "base_fields_set must be able to store non-indexed fields");
static_assert(sizeof(std::bitset<kMaxIndexes>) == 32, "Expecting no overhead from std::bitset");
using FieldsPath = std::variant<TagsPath, IndexedTagsPath>;

using ScalarIndexesSetT = std::bitset<kMaxIndexes>;

class [[nodiscard]] IndexesFieldsSet {
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

class [[nodiscard]] FieldsSet : protected base_fields_set {
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
			base_fields_set::insert(cbegin(), IndexValueType::SetByJsonPath);
			tagsPaths_.insert(tagsPaths_.cbegin(), std::move(tagsPath));
		}
	}

	void push_front(const TagsPath& tagsPath) {
		if (!contains(tagsPath)) {
			base_fields_set::insert(cbegin(), IndexValueType::SetByJsonPath);
			tagsPaths_.emplace(tagsPaths_.cbegin(), tagsPath);
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
			base_fields_set::insert(cbegin(), f);
		}
	}

	void erase(int f) {
		const bool byJsonPath = (f < 0);
		if (byJsonPath || contains(f)) {
			auto it = std::find(cbegin(), cend(), f);
			assertrx(it != cend());
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
	bool contains(const TagsPath& tagsPath) const {
		for (const FieldsPath& path : tagsPaths_) {
			if (std::visit(overloaded{[&tagsPath](const TagsPath& path) { return path == tagsPath; },
									  [&tagsPath](const IndexedTagsPath& path) { return path.Compare(tagsPath); }},
						   path)) {
				return true;
			}
		}
		return false;
	}
	bool contains(const IndexedTagsPath& tagsPath) const {
		for (const FieldsPath& path : tagsPaths_) {
			if (std::visit(
					overloaded{[&tagsPath](const TagsPath& path) { return tagsPath.Compare(path); },
							   [&tagsPath](const IndexedTagsPath& path) { return Compare<IgnoreAllOmittedIndexes>(path, tagsPath); }},
					path)) {
				return true;
			}
		}
		return false;
	}
	bool contains(const FieldsPath& fieldsPath) const {
		return std::visit([&](const auto& fp) { return contains(fp); }, fieldsPath);
	}
	bool match(const TagsPath& tagsPath) const {
		if (tagsPaths_.empty()) {
			return true;
		}
		for (auto& path : tagsPaths_) {
			if (std::visit(overloaded{[&tagsPath](const TagsPath& path) { return comparePaths(tagsPath, path); },
									  [&tagsPath](const IndexedTagsPath& path) { return comparePaths(path, tagsPath); }},
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
			if (std::visit(overloaded{[&tagsPath](const TagsPath& path) { return comparePaths(tagsPath, path); },
									  [&tagsPath](const IndexedTagsPath& path) { return comparePaths(tagsPath, path); }},
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
	bool isTagsPathIndexed(size_t idx) const {
		assertrx(idx < tagsPaths_.size());
		return std::visit(overloaded{[](const TagsPath&) { return false; }, [](const IndexedTagsPath&) { return true; }}, tagsPaths_[idx]);
	}
	const TagsPath& getTagsPath(size_t idx) const& { return std::get<TagsPath>(tagsPaths_[idx]); }
	const TagsPath& getTagsPath(size_t idx) const&& = delete;
	const IndexedTagsPath& getIndexedTagsPath(size_t idx) const& { return std::get<IndexedTagsPath>(tagsPaths_[idx]); }
	const IndexedTagsPath& getIndexedTagsPath(size_t idx) const&& = delete;
	const h_vector<FieldsPath, 1>& getAllTagsPaths() const& noexcept { return tagsPaths_; }
	const auto& getAllTagsPaths() const&& = delete;
	const FieldsPath& getFieldsPath(size_t idx) const& { return tagsPaths_[idx]; }
	const FieldsPath& getFieldsPath(size_t idx) const&& = delete;
	const std::string& getJsonPath(size_t idx) const& noexcept { return jsonPaths_[idx]; }
	const std::string& getJsonPath(size_t idx) const&& = delete;

	bool operator==(const FieldsSet& f) const {
		return mask_ == f.mask_ && jsonPaths_ == f.jsonPaths_ && tagsPaths_.size() == f.tagsPaths_.size() &&
			   std::equal(tagsPaths_.cbegin(), tagsPaths_.cend(), f.tagsPaths_.cbegin(),
						  [](const FieldsPath& leftTagsPath, const FieldsPath& rightTagsPath) {
							  return std::visit(overloaded{
													[](const TagsPath& lhs, const TagsPath& rhs) noexcept { return lhs == rhs; },
													[](const TagsPath&, const IndexedTagsPath&) noexcept { return false; },
													[](const IndexedTagsPath&, const TagsPath&) noexcept { return false; },
													[](const IndexedTagsPath& lhs, const IndexedTagsPath& rhs) noexcept {
														return Compare<IgnoreAllOmittedIndexes>(lhs, rhs);
													},
												},
												leftTagsPath, rightTagsPath);
						  });
	}
	bool operator!=(const FieldsSet& f) const noexcept { return !(*this == f); }

	template <typename Os>
	void Dump(Os& os, DumpWithMask withMask) const;
	std::string ToString(DumpWithMask withMask) const;

private:
	template <typename F>
	void pushBack(F&& fieldPath) {
		if (!contains(fieldPath)) {
			base_fields_set::push_back(IndexValueType::SetByJsonPath);
			tagsPaths_.emplace_back(std::forward<F>(fieldPath));
		}
	}
	static bool comparePaths(const TagsPath& lhs, const TagsPath& rhs) noexcept {
		size_t i = 0, count = std::min(lhs.size(), rhs.size());
		for (; i < count && lhs[i] == rhs[i]; ++i) {
		}
		return (i == count);
	}
	template <unsigned hvSize>
	static bool comparePaths(const IndexedTagsPathImpl<hvSize>& lhs, const TagsPath& rhs) noexcept {
		for (size_t li = 0, ri = 0, ls = lhs.size(), rs = rhs.size(); li < ls && ri < rs; ++li) {
			if (lhs[li].IsTagName()) {
				if (lhs[li].GetTagName() != rhs[ri]) {
					return false;
				}
				++ri;
			}
		}
		return true;
	}
	template <unsigned hvSize>
	static bool comparePaths(const TagsPath& lhs, const IndexedTagsPathImpl<hvSize>& rhs) noexcept {
		return comparePaths(rhs, lhs);
	}
	template <unsigned lHvSize, unsigned rHvSize>
	static bool comparePaths(const IndexedTagsPathImpl<lHvSize>& lhs, const IndexedTagsPathImpl<rHvSize>& rhs) noexcept {
		return lhs.ComparePrefix(rhs);
	}
	[[noreturn]] void throwMaxValueError(int f);

	std::bitset<kMaxIndexes> mask_;
	h_vector<FieldsPath, 1> tagsPaths_;
	/// Json paths to non indexed fields.
	/// Necessary only for composite full text
	/// indexes. There is a connection with
	/// tagsPaths_: order and amount of elements.
	h_vector<std::string, 1> jsonPaths_;

	template <typename Os>
	class [[nodiscard]] DumpFieldsPath;
};

}  // namespace reindexer
