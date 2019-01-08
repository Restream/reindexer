#include "indexopts.h"

CollateOpts::CollateOpts(uint8_t mode) : mode(mode) {}

CollateOpts::CollateOpts(const std::string& sortOrderUTF8) : mode(CollateCustom), sortOrderTable(sortOrderUTF8) {}

IndexOpts::IndexOpts(uint8_t flags, CollateMode mode) : options(flags), collateOpts_(mode) {}

IndexOpts::IndexOpts(const std::string& sortOrderUTF8, uint8_t flags) : options(flags), collateOpts_(sortOrderUTF8) {}

bool IndexOpts::IsEqual(const IndexOpts& other, bool skipConfig) const {
	return options == other.options && (skipConfig || config == other.config) && collateOpts_.mode == other.collateOpts_.mode &&
		   collateOpts_.sortOrderTable.GetSortOrderCharacters() == other.collateOpts_.sortOrderTable.GetSortOrderCharacters();
}

bool IndexOpts::IsPK() const { return options & kIndexOptPK; }
bool IndexOpts::IsArray() const { return options & kIndexOptArray; }
bool IndexOpts::IsDense() const { return options & kIndexOptDense; }
bool IndexOpts::IsSparse() const { return options & kIndexOptSparse; }
bool IndexOpts::hasConfig() const { return !config.empty(); }
CollateMode IndexOpts::GetCollateMode() const { return static_cast<CollateMode>(collateOpts_.mode); }

IndexOpts& IndexOpts::PK(bool value) {
	options = value ? options | kIndexOptPK : options & ~(kIndexOptPK);
	return *this;
}

IndexOpts& IndexOpts::Array(bool value) {
	options = value ? options | kIndexOptArray : options & ~(kIndexOptArray);
	return *this;
}

IndexOpts& IndexOpts::Dense(bool value) {
	options = value ? options | kIndexOptDense : options & ~(kIndexOptDense);
	return *this;
}

IndexOpts& IndexOpts::Sparse(bool value) {
	options = value ? options | kIndexOptSparse : options & ~(kIndexOptSparse);
	return *this;
}

IndexOpts& IndexOpts::SetCollateMode(CollateMode mode) {
	collateOpts_.mode = mode;
	return *this;
}

IndexOpts& IndexOpts::SetConfig(const std::string& newConfig) {
	config = newConfig;
	return *this;
}
