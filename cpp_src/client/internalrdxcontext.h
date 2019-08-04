#pragma once

#include <chrono>
#include "tools/errors.h"

namespace reindexer {

namespace client {

using std::chrono::milliseconds;

class InternalRdxContext {
public:
	typedef std::function<void(const Error &err)> Completion;
	InternalRdxContext(Completion cmpl = nullptr, milliseconds execTimeout = milliseconds(0)) noexcept
		: cmpl_(cmpl), execTimeout_((execTimeout.count() < 0) ? milliseconds(0) : execTimeout) {}

	InternalRdxContext WithCompletion(Completion cmpl) const noexcept { return InternalRdxContext(cmpl, execTimeout_); }
	InternalRdxContext WithTimeout(milliseconds execTimeout) const noexcept { return InternalRdxContext(cmpl_, execTimeout); }

	Completion cmpl() const noexcept { return cmpl_; }
	milliseconds execTimeout() const noexcept { return execTimeout_; }

private:
	Completion cmpl_;
	milliseconds execTimeout_;
};

}  // namespace client
}  // namespace reindexer
