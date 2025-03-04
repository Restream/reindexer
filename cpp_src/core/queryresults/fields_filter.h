#pragma once

#include "core/payload/fieldsset.h"

namespace reindexer {

class FieldsNamesFilter;
class NamespaceImpl;

class [[nodiscard]] FieldsFilter {
public:
	FieldsFilter() noexcept = default;
	FieldsFilter(const FieldsNamesFilter&, const NamespaceImpl&);

	template <unsigned hvSize>
	[[nodiscard]] bool Match(const IndexedTagsPathImpl<hvSize>& tagsPath) const noexcept {
		return allRegularFields_ || regularFields_.match(tagsPath);
	}
	[[nodiscard]] bool HasTagsPaths() const noexcept {
		return (!allRegularFields_ && regularFields_.getTagsPathsLength()) || (!allVectorFields_ && vectorFields_.getTagsPathsLength());
	}
	[[nodiscard]] bool HasStars() const noexcept { return allRegularFields_ || allVectorFields_; }
	[[nodiscard]] bool HasVectors() const noexcept { return allVectorFields_ || !vectorFields_.empty(); }

	[[nodiscard]] bool ContainsVector(int idx) const noexcept { return allVectorFields_ || vectorFields_.contains(idx); }

	[[nodiscard]] bool ContainsVector(const TagsPath& path) const noexcept { return allVectorFields_ || vectorFields_.contains(path); }

	[[nodiscard]] const FieldsSet& RegularFields() const& noexcept {
		assertrx_dbg(!allRegularFields_);
		return regularFields_;
	}
	[[nodiscard]] const FieldsSet* TryRegularFields() const& noexcept { return allRegularFields_ ? nullptr : &regularFields_; }
	[[nodiscard]] const FieldsSet& VectorFields() const& noexcept {
		assertrx_dbg(!allVectorFields_);
		return vectorFields_;
	}
	[[nodiscard]] const FieldsSet* TryVectorFields() const& noexcept { return allVectorFields_ ? nullptr : &vectorFields_; }

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

	FieldsSet regularFields_;
	FieldsSet vectorFields_;
	bool allRegularFields_{true};
	bool allVectorFields_{false};
};

}  // namespace reindexer
