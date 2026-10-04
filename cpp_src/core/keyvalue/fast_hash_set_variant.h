#pragma once

#include "estl/fast_hash_set.h"
#include "variant.h"

namespace reindexer {

class [[nodiscard]] fast_hash_set_variant : public fast_hash_set<Variant, std::hash<Variant>, Variant::EqualTo, Variant::Less> {
	using Base = fast_hash_set<Variant, std::hash<Variant>, Variant::EqualTo, Variant::Less>;

public:
	fast_hash_set_variant(const CollateOpts& collate)
		: Base{16, std::hash<Variant>{}, Variant::EqualTo{collate}, typename Base::allocator_type{}, Variant::Less{collate}} {}
};

}  // namespace reindexer
