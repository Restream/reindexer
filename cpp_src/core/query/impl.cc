#include "impl.h"
#include "core/query/query.h"
#include "core/query/query_impl.h"

namespace reindexer::impl {

impl::Query Impl<reindexer::Query&&>::operator*() && {
	if (!query_.state_.ok()) {
		throw query_.state_;
	}
	if (query_.impl_.use_count() == 1) {
		return std::move(*query_.impl_);
	} else {
		return *query_.impl_;
	}
}

void Impl<reindexer::Query&>::operator=(impl::Query&& other) && {
	query_.state_ = {};
	query_.impl_ = std::make_shared<impl::Query>(std::move(other));
}

}  // namespace reindexer::impl
