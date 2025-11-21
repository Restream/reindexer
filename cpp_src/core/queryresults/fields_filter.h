#pragma once

#include "core/payload/fieldsset.h"
#include "estl/concepts.h"

namespace reindexer {

class FieldsNamesFilter;
class NamespaceImpl;

class [[nodiscard]] FieldsFilter {
public:
	FieldsFilter() noexcept = default;
	FieldsFilter(const FieldsNamesFilter&, const NamespaceImpl&);
	template <concepts::ConvertibleToString Str>
	FieldsFilter(const Str& field, const NamespaceImpl&);
	FieldsFilter(std::span<const std::string> fields, const NamespaceImpl&);

	template <unsigned hvSize>
	bool Match(const IndexedTagsPathImpl<hvSize>& tagsPath) const noexcept {
		return allRegularFields_ || regularFields_.match(tagsPath);
	}
	bool HasTagsPaths() const noexcept {
		return (!allRegularFields_ && regularFields_.getTagsPathsLength()) || (!allVectorFields_ && vectorFields_.getTagsPathsLength());
	}
	bool HasStars() const noexcept { return allRegularFields_ || allVectorFields_; }
	bool HasVectors() const noexcept { return allVectorFields_ || !vectorFields_.empty(); }

	bool ContainsVector(int idx) const noexcept { return allVectorFields_ || vectorFields_.contains(idx); }

	bool ContainsVector(const TagsPath& path) const noexcept { return allVectorFields_ || vectorFields_.contains(path); }

	const FieldsSet& RegularFields() const& noexcept {
		assertrx_dbg(!allRegularFields_);
		return regularFields_;
	}
	const FieldsSet* TryRegularFields() const& noexcept { return allRegularFields_ ? nullptr : &regularFields_; }
	const FieldsSet& VectorFields() const& noexcept {
		assertrx_dbg(!allVectorFields_);
		return vectorFields_;
	}
	const FieldsSet* TryVectorFields() const& noexcept { return allVectorFields_ ? nullptr : &vectorFields_; }

	static FieldsFilter AllFields() noexcept { return FieldsFilter{true, true}; }

	template <typename P>
	static FieldsFilter FromPath(P&& path) {
		return std::forward<P>(path);
	}

	auto RegularFields() const&& = delete;
	auto TryRegularFields() const&& = delete;
	auto VectorFields() const&& = delete;
	auto TryVectorFields() const&& = delete;

	std::string Dump() const;

private:
	FieldsFilter(bool allReg, bool allVec) noexcept : allRegularFields_{allReg}, allVectorFields_{allVec} {}

	template <typename P>
	FieldsFilter(P&& path) : regularFields_{{std::forward<P>(path)}}, allRegularFields_{false} {}

	template <concepts::ConvertibleToString Str>
	void add(const Str& field, const NamespaceImpl&);

	void fill(std::span<const std::string> fields, const NamespaceImpl&);

	FieldsSet regularFields_;
	FieldsSet vectorFields_;
	bool allRegularFields_{true};
	bool allVectorFields_{false};
};

}  // namespace reindexer
