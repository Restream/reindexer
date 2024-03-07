#include "indexopts.h"
#include <ostream>
#include "type_consts_helpers.h"

CollateOpts::CollateOpts(CollateMode mode) : mode(mode) {}

CollateOpts::CollateOpts(const std::string& sortOrderUTF8) : mode(CollateCustom), sortOrderTable(sortOrderUTF8) {}

template <typename T>
void CollateOpts::Dump(T& os) const {
	using namespace reindexer;
	os << mode;
	if (mode == CollateCustom) {
		os << ": [" << sortOrderTable.GetSortOrderCharacters() << ']';
	}
}
template void CollateOpts::Dump<std::ostream>(std::ostream&) const;

IndexOpts::IndexOpts(uint8_t flags, CollateMode mode, RTreeIndexType rtreeType)
	: options(flags), collateOpts_(mode), rtreeType_(rtreeType) {}

IndexOpts::IndexOpts(const std::string& sortOrderUTF8, uint8_t flags, RTreeIndexType rtreeType)
	: options(flags), collateOpts_(sortOrderUTF8), rtreeType_(rtreeType) {}

bool IndexOpts::IsEqual(const IndexOpts& other, IndexComparison cmpType) const noexcept {
	return options == other.options && (cmpType == IndexComparison::SkipConfig || config == other.config) &&
		   collateOpts_.mode == other.collateOpts_.mode &&
		   collateOpts_.sortOrderTable.GetSortOrderCharacters() == other.collateOpts_.sortOrderTable.GetSortOrderCharacters() &&
		   rtreeType_ == other.rtreeType_;
}

bool IndexOpts::IsPK() const noexcept { return options & kIndexOptPK; }
bool IndexOpts::IsArray() const noexcept { return options & kIndexOptArray; }
bool IndexOpts::IsDense() const noexcept { return options & kIndexOptDense; }
bool IndexOpts::IsSparse() const noexcept { return options & kIndexOptSparse; }
bool IndexOpts::hasConfig() const noexcept { return !config.empty(); }
CollateMode IndexOpts::GetCollateMode() const noexcept { return static_cast<CollateMode>(collateOpts_.mode); }

IndexOpts& IndexOpts::PK(bool value) & noexcept {
	options = value ? options | kIndexOptPK : options & ~(kIndexOptPK);
	return *this;
}

IndexOpts& IndexOpts::Array(bool value) & noexcept {
	options = value ? options | kIndexOptArray : options & ~(kIndexOptArray);
	return *this;
}

IndexOpts& IndexOpts::Dense(bool value) & noexcept {
	options = value ? options | kIndexOptDense : options & ~(kIndexOptDense);
	return *this;
}

IndexOpts& IndexOpts::Sparse(bool value) & noexcept {
	options = value ? options | kIndexOptSparse : options & ~(kIndexOptSparse);
	return *this;
}

IndexOpts& IndexOpts::RTreeType(RTreeIndexType value) & noexcept {
	rtreeType_ = value;
	return *this;
}

IndexOpts& IndexOpts::SetCollateMode(CollateMode mode) & noexcept {
	collateOpts_.mode = mode;
	return *this;
}

template <typename T>
void IndexOpts::Dump(T& os) const {
	os << '{';
	bool needComma = false;
	if (IsPK()) {
		os << "PK";
		needComma = true;
	}
	if (IsArray()) {
		if (needComma) os << ", ";
		os << "Array";
		needComma = true;
	}
	if (IsDense()) {
		if (needComma) os << ", ";
		os << "Dense";
		needComma = true;
	}
	if (IsSparse()) {
		if (needComma) os << ", ";
		os << "Sparse";
		needComma = true;
	}
	if (needComma) os << ", ";
	os << RTreeType();
	if (hasConfig()) {
		os << ", config: " << config;
	}
	os << ", collate: ";
	collateOpts_.Dump(os);
	os << '}';
}

template void IndexOpts::Dump<std::ostream>(std::ostream&) const;
