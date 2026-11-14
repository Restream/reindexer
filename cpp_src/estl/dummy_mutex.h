#pragma once

#include "thread_annotation_attributes.h"

namespace reindexer {

class [[nodiscard]] RX_CAPABILITY("mutex") DummyMutex {
public:
	void lock() const noexcept {}
	void lock_shared() const noexcept {}
	void unlock() const noexcept {}
	void unlock_shared() const noexcept {}
	const DummyMutex& operator!() const& { return *this; }
	auto operator!() const&& = delete;
};

}  // namespace reindexer
