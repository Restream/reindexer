#pragma once

#include "core/cancelcontextpool.h"
#include "core/cbinding/reindexer_ctypes.h"
#include "core/reindexer.h"

namespace reindexer {

class [[nodiscard]] CGOCtxPool {
public:
	CGOCtxPool() = delete;
	CGOCtxPool(size_t baseSize) : ctxPool_(baseSize) {}

	IRdxCancelContext* getContext(reindexer_ctx_info ctxInfo) noexcept { return ctxPool_.getContext(ctxInfo.ctx_id); }
	bool cancelContext(reindexer_ctx_info ctxInfo, CancelType how) noexcept { return ctxPool_.cancelContext(ctxInfo.ctx_id, how); }
	bool removeContext(reindexer_ctx_info ctxInfo) noexcept { return ctxPool_.removeContext(ctxInfo.ctx_id); }

private:
	ContextsPoolImpl<CancelContextImpl> ctxPool_;
};

class [[nodiscard]] CGORdxCtxKeeper {
public:
	CGORdxCtxKeeper() = delete;
	CGORdxCtxKeeper(const CGORdxCtxKeeper&) = delete;
	CGORdxCtxKeeper(uintptr_t rx, reindexer_ctx_info ctxInfo, CGOCtxPool& pool) noexcept
		: pool_(pool),
		  ctx_(pool.getContext(ctxInfo)),
		  ctxInfo_(ctxInfo),
		  rdx_(reinterpret_cast<Reindexer*>(rx)->WithTimeout(milliseconds(ctxInfo.exec_timeout)).WithContext(ctx_)) {}

	~CGORdxCtxKeeper() {
		if (ctx_) {
			pool_.removeContext(ctxInfo_);
		}
	}

	Reindexer& db() noexcept { return rdx_; }

private:
	CGOCtxPool& pool_;
	IRdxCancelContext* ctx_;
	reindexer_ctx_info ctxInfo_;
	Reindexer rdx_;
};

}  // namespace reindexer
