#pragma once

#include "core/query/impl.h"

namespace reindexer {

inline bool operator==(const Query& lhs, const Query& rhs) {
	using impl::Impl;
	return *Impl{lhs} == *Impl{rhs};
}

inline bool operator==(const Query& lhs, const impl::Query& rhs) {
	using impl::Impl;
	return *Impl{lhs} == rhs;
}

}  // namespace reindexer
