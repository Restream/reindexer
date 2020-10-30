#pragma once

#include "sortingprioritiestable.h"

struct CollateOpts {
	explicit CollateOpts(uint8_t mode = CollateNone);
	explicit CollateOpts(const std::string& sortOrderUTF8);

	uint8_t mode = CollateNone;
	reindexer::SortingPrioritiesTable sortOrderTable;
};

/// Cpp version of IndexOpts: includes
/// sort order table which is not possible
/// to link in C-GO version because of templates
/// in memory.h and unordered_map.h
struct IndexOpts {
	explicit IndexOpts(uint8_t flags = 0, CollateMode mode = CollateNone);
	explicit IndexOpts(const std::string& sortOrderUTF8, uint8_t flags = 0);

	bool IsPK() const noexcept;
	bool IsArray() const noexcept;
	bool IsDense() const noexcept;
	bool IsSparse() const noexcept;
	bool IsRTreeLinear() const noexcept;
	bool hasConfig() const noexcept;

	IndexOpts& PK(bool value = true) noexcept;
	IndexOpts& Array(bool value = true) noexcept;
	IndexOpts& Dense(bool value = true) noexcept;
	IndexOpts& Sparse(bool value = true) noexcept;
	IndexOpts& RTreeLinear(bool value = true) noexcept;
	IndexOpts& RTreeQuadratic(bool value = true) noexcept { return RTreeLinear(!value); }
	IndexOpts& SetCollateMode(CollateMode mode) noexcept;
	IndexOpts& SetConfig(const std::string& config);
	CollateMode GetCollateMode() const noexcept;

	bool operator==(const IndexOpts& other) const { return IsEqual(other, false); }
	bool IsEqual(const IndexOpts& other, bool skipConfig) const;

	uint8_t options;
	CollateOpts collateOpts_;
	std::string config;
};
