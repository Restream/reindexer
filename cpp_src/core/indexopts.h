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

enum class IndexComparison { WithConfig, SkipConfig };

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

	IndexOpts& PK(bool value = true) & noexcept;
	[[nodiscard]] IndexOpts&& PK(bool value = true) && noexcept { return std::move(PK(value)); }
	IndexOpts& Array(bool value = true) & noexcept;
	[[nodiscard]] IndexOpts&& Array(bool value = true) && noexcept { return std::move(Array(value)); }
	IndexOpts& Dense(bool value = true) & noexcept;
	[[nodiscard]] IndexOpts&& Dense(bool value = true) && noexcept { return std::move(Dense(value)); }
	IndexOpts& Sparse(bool value = true) & noexcept;
	[[nodiscard]] IndexOpts&& Sparse(bool value = true) && noexcept { return std::move(Sparse(value)); }
	IndexOpts& RTreeType(RTreeIndexType) & noexcept;
	[[nodiscard]] IndexOpts&& RTreeType(RTreeIndexType type) && noexcept { return std::move(RTreeType(type)); }
	IndexOpts& SetCollateMode(CollateMode mode) & noexcept;
	[[nodiscard]] IndexOpts&& SetCollateMode(CollateMode mode) && noexcept { return std::move(SetCollateMode(mode)); }
	template <typename Str, std::enable_if_t<std::is_assignable_v<std::string, Str>>* = nullptr>
	IndexOpts& SetConfig(Str&& conf) & {
		config = std::forward<Str>(conf);
		return *this;
	}
	template <typename Str, std::enable_if_t<std::is_assignable_v<std::string, Str>>* = nullptr>
	[[nodiscard]] IndexOpts&& SetConfig(Str&& config) && {
		return std::move(SetConfig(std::forward<Str>(config)));
	}
	CollateMode GetCollateMode() const noexcept;

	bool IsEqual(const IndexOpts& other, IndexComparison cmpType) const noexcept;

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
	}
	std::abort();
}
