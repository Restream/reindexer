#pragma once

#include <gtest/gtest.h>
#include "core/cancelcontextpool.h"

namespace reindexer {
std::ostream& operator<<(std::ostream& os, const reindexer::CancelType& cancel) { return os << static_cast<int>(cancel); }
}  // namespace reindexer

namespace reindexer_tests {

using std::unique_ptr;
using reindexer::ContextsPoolImpl;
using reindexer::CancelContextImpl;
using reindexer::IRdxCancelContext;

namespace CGOCtxPoolTests {

class [[nodiscard]] CGOCtxPoolApi : public ::testing::Test {
protected:
	enum class [[nodiscard]] MultiThreadTestMode { Simple, Synced };

	unique_ptr<ContextsPoolImpl<CancelContextImpl>> createCtxPool(size_t baseSize) {
		return unique_ptr<ContextsPoolImpl<CancelContextImpl>>(new ContextsPoolImpl<CancelContextImpl>(baseSize));
	}
	IRdxCancelContext* getAndValidateCtx(uint64_t ctxID, ContextsPoolImpl<CancelContextImpl>& pool) {
		auto ctx = pool.getContext(ctxID);
		if (ctx) {
			EXPECT_EQ(ctx->GetCancelType(), reindexer::CancelType::None);
		}
		return ctx;
	}

	void multiThreadTest(size_t threadsCount, MultiThreadTestMode mode);
};

}  // namespace CGOCtxPoolTests

}  // namespace reindexer_tests
