#pragma once

#include "core/type_consts.h"
#include "core/definitions/sortingprioritiestable.h"

namespace reindexer {

struct [[nodiscard]] CollateOpts {
	explicit CollateOpts(CollateMode mode = CollateNone) noexcept : mode(mode) {}
	explicit CollateOpts(const std::string& sortOrderUTF8);

	CollateMode mode = CollateNone;
	reindexer::SortingPrioritiesTable sortOrderTable;
	template <typename T>
	void Dump(T& os) const;
};

}
