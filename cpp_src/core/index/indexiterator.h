#pragma once

#include "core/type_consts.h"
#include "estl/intrusive_ptr.h"

namespace reindexer {

class IndexIteratorBase {
public:
	virtual ~IndexIteratorBase() = default;
	virtual void Start(bool reverse) = 0;
	virtual IdType Value() const = 0;
	virtual bool Next() = 0;
	virtual void ExcludeLastSet() = 0;
	virtual size_t GetMaxIterations(size_t limitIters) = 0;
	virtual void SetMaxIterations(size_t iters) = 0;
};

class IndexIterator : public intrusive_atomic_rc_wrapper<IndexIteratorBase> {
public:
	using Ptr = intrusive_ptr<IndexIterator>;
};

}  // namespace reindexer
