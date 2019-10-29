#pragma once

#include <chrono>
#include "core/cancelcontextpool.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

class InternalRdxContext {
public:
	typedef std::function<void(const Error& err)> Completion;
	explicit InternalRdxContext(const IRdxCancelContext* cancelCtx, Completion cmpl = nullptr,
								milliseconds execTimeout = milliseconds(0)) noexcept
		: cmpl_(cmpl), execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout), cancelCtx_(cancelCtx) {}
	explicit InternalRdxContext(Completion cmpl = nullptr, milliseconds execTimeout = milliseconds(0)) noexcept
		: cmpl_(cmpl), execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout), cancelCtx_(nullptr) {}

	InternalRdxContext WithCancelContext(const IRdxCancelContext* cancelCtx) noexcept {
		return InternalRdxContext(cancelCtx, cmpl_, execTimeout_);
	}
	InternalRdxContext WithCompletion(Completion cmpl, InternalRdxContext&) noexcept { return InternalRdxContext(cmpl, execTimeout_); }
	InternalRdxContext WithCompletion(Completion cmpl) const noexcept { return InternalRdxContext(cmpl, execTimeout_); }
	InternalRdxContext WithTimeout(milliseconds execTimeout) const noexcept { return InternalRdxContext(cmpl_, execTimeout); }

	Completion cmpl() const noexcept { return cmpl_; }
	milliseconds execTimeout() const noexcept { return execTimeout_; }
	const IRdxCancelContext* getCancelCtx() const noexcept { return cancelCtx_; }

private:
	Completion cmpl_;
	milliseconds execTimeout_;
	const IRdxCancelContext* cancelCtx_;
};

}  // namespace client
}  // namespace reindexer
