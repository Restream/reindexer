#pragma once

#include <memory>
#include "estl/chunk.h"

namespace reindexer {

class [[nodiscard]] MemBuf {
public:
	MemBuf() = default;
	MemBuf(std::unique_ptr<uint8_t[]>&& ptr, size_t size) noexcept : data_{std::move(ptr)}, size_{size} {}
	MemBuf(const MemBuf&) = delete;
	MemBuf(MemBuf&&) noexcept = default;
	MemBuf& operator=(const MemBuf&) = delete;
	MemBuf& operator=(MemBuf&&) noexcept = default;

	chunk DetachChunk() noexcept { return chunk(data_.release(), size_, size_); }
	void Reset() noexcept {
		data_.reset();
		size_ = 0;
	}
	explicit operator bool() const noexcept { return data_.get(); }
	const uint8_t* Get() const noexcept { return data_.get(); }

private:
	std::unique_ptr<uint8_t[]> data_;
	size_t size_{0};
};

}  // namespace reindexer
