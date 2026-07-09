#pragma once

#include <exception>

namespace reindexer::serialize_helpers {

// The End() call may throw if the internal WrSerializer is unable to allocate memory due to logical
// (GrowthPolicy) or system (std::bad_alloc) limitations. Checking std::uncaught_exceptions() allows us
// to avoid throwing an exception in scenarios where we're already handling another exception.
void tryAppendEnd(auto& builder) {
	if (std::uncaught_exceptions() == 0) {
		builder.End();
	} else {
		try {
			builder.End();
			// NOLINTBEGIN(bugprone-empty-catch)
		} catch (...) {
			// Exception must be ignored during stack unwinding
		}
		// NOLINTEND(bugprone-empty-catch)
	}
}

} // namespace reindexer::serialize_helpers