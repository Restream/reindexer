#pragma once

#include "sortingprioritiestable.h"

struct CollateOpts {
	explicit CollateOpts(CollateMode mode = CollateNone);
	explicit CollateOpts(const std::string& sortOrderUTF8);

	CollateMode mode = CollateNone;
	reindexer::SortingPrioritiesTable sortOrderTable;
	template <typename T>
	void Dump(T& os) const;
};

/// Cpp version of IndexOpts: includes
/// sort order table which is not possible
/// to link in C-GO version because of templates
/// in memory.h and unordered_map.h
struct IndexOpts {
	enum RTreeIndexType : uint8_t { Linear = 0, Quadratic = 1, Greene = 2, RStar = 3 };
	explicit IndexOpts(uint8_t flags = 0, CollateMode mode = CollateNone, RTreeIndexType = RStar);
	explicit IndexOpts(const std::string& sortOrderUTF8, uint8_t flags = 0, RTreeIndexType = RStar);

	bool IsPK() const noexcept;
	bool IsArray() const noexcept;
	bool IsDense() const noexcept;
	bool IsSparse() const noexcept;
	RTreeIndexType RTreeType() const noexcept { return rtreeType_; }
	bool hasConfig() const noexcept;

	IndexOpts& PK(bool value = true) noexcept;
	IndexOpts& Array(bool value = true) noexcept;
	IndexOpts& Dense(bool value = true) noexcept;
	IndexOpts& Sparse(bool value = true) noexcept;
	IndexOpts& RTreeType(RTreeIndexType) noexcept;
	IndexOpts& SetCollateMode(CollateMode mode) noexcept;
	IndexOpts& SetConfig(const std::string& config);
	CollateMode GetCollateMode() const noexcept;

	bool operator==(const IndexOpts& other) const { return IsEqual(other, false); }
	bool IsEqual(const IndexOpts& other, bool skipConfig) const;

	template <typename T>
	void Dump(T& os) const;

	uint8_t options;
	CollateOpts collateOpts_;
	std::string config;
	RTreeIndexType rtreeType_ = RStar;
};

template <typename T>
T& operator<<(T& os, IndexOpts::RTreeIndexType t) {
	switch (t) {
		case IndexOpts::Linear:
			return os << "Linear";
		case IndexOpts::Quadratic:
			return os << "Quadratic";
		case IndexOpts::Greene:
			return os << "Greene";
		case IndexOpts::RStar:
			return os << "RStar";
		default:
			abort();
	}
}
