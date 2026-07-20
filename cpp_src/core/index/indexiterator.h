#pragma once

#include "core/type_consts.h"
#include "estl/intrusive_ptr.h"

namespace reindexer {

class [[nodiscard]] IndexIteratorBase {
public:
	virtual ~IndexIteratorBase() = default;
	virtual void Start(bool reverse) = 0;
	virtual IdType Value() const noexcept = 0;
	virtual bool Next() noexcept = 0;
	virtual void ExcludeLastSet() noexcept = 0;
	virtual size_t GetMaxIterations(size_t limitIters) noexcept = 0;
	virtual void SetMaxIterations(size_t iters) noexcept = 0;
};

class [[nodiscard]] IndexIterator : public intrusive_atomic_rc_wrapper<IndexIteratorBase> {
public:
	using Ptr = intrusive_ptr<IndexIterator>;
};

}  // namespace reindexer
