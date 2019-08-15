#pragma once

#include <gtest/gtest.h>
#include "core/cbinding/cgoctxpool.h"

using std::unique_ptr;
using reindexer::ContextsPool;
using reindexer::CGORdxContext;
using reindexer::IRdxCancelContext;

namespace reindexer {
std::ostream& operator<<(std::ostream& os, const CancelType& cancel) { return os << static_cast<int>(cancel); }
}  // namespace reindexer

namespace CGOCtxPoolTests {

class CGOCtxPoolApi : public ::testing::Test {
public:
	CGOCtxPoolApi() {}
	virtual ~CGOCtxPoolApi() {}

protected:
	enum class MultiThreadTestMode { Simple, Synced };

	void SetUp() {}
	void TearDown() {}

	unique_ptr<ContextsPool<CGORdxContext>> createCtxPool(size_t baseSize) {
		return unique_ptr<ContextsPool<CGORdxContext>>(new ContextsPool<CGORdxContext>(baseSize));
	}
	IRdxCancelContext* getAndValidateCtx(uint64_t ctxID, ContextsPool<CGORdxContext>& pool) {
		auto ctx = pool.getContext(ctxID);
		if (ctx) {
			EXPECT_EQ(ctx->checkCancel(), reindexer::CancelType::None);
		}
		return ctx;
	}

	void multiThreadTest(size_t threadsCount, MultiThreadTestMode mode);
};

}  // namespace CGOCtxPoolTests
