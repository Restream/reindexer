#pragma once

#include <stddef.h>
#include <atomic>
#include <iosfwd>
#include "tools/lsn.h"

#ifdef RX_WITH_STDLIB_DEBUG
#include "tools/assertrx.h"
#endif	// RX_WITH_STDLIB_DEBUG

namespace reindexer {

// The full item's payload object. It must be speed & size optimized
class [[nodiscard]] PayloadValue {
public:
	typedef std::atomic<int32_t> refcounter;
	struct [[nodiscard]] dataHeader {
		dataHeader() noexcept : refcount(1), cap(0) {}

#ifdef RX_WITH_STDLIB_DEBUG
		~dataHeader() { assertrx_dbg(refcount.load(std::memory_order_acquire) == 0); }
#else	// RX_WITH_STDLIB_DEBUG
		~dataHeader() = default;
#endif	// RX_WITH_STDLIB_DEBUG
		refcounter refcount;
		unsigned cap;
		lsn_t lsn;
	};

	PayloadValue() noexcept : p_(nullptr) {}
	PayloadValue(const PayloadValue& other) noexcept : p_(other.p_) {
		if (p_) {
			header()->refcount.fetch_add(1, std::memory_order_relaxed);
		}
	}
	// Alloc payload store with size, and copy data from another array
	PayloadValue(size_t size, const uint8_t* ptr = nullptr, size_t cap = 0);
	~PayloadValue() { release(); }
	PayloadValue& operator=(const PayloadValue& other) noexcept {
		if (&other != this) {
			release();
			p_ = other.p_;
			if (p_) {
				header()->refcount.fetch_add(1, std::memory_order_relaxed);
			}
		}
		return *this;
	}
	PayloadValue(PayloadValue&& other) noexcept : p_(other.p_) { other.p_ = nullptr; }
	PayloadValue& operator=(PayloadValue&& other) noexcept {
		if (&other != this) {
			release();
			p_ = other.p_;
			other.p_ = nullptr;
		}

		return *this;
	}

	// Clone if data is shared for copy-on-write.
	void Clone(size_t size = 0);
	// Resize
	void Resize(size_t oldSize, size_t newSize);
	// Get data pointer
	uint8_t* Ptr() const noexcept { return p_ + sizeof(dataHeader); }
	void SetLSN(lsn_t lsn) noexcept { header()->lsn = lsn; }
	lsn_t GetLSN() const noexcept { return p_ ? header()->lsn : lsn_t(); }
	bool IsFree() const noexcept { return bool(p_ == nullptr); }
	void Free() noexcept { release(); }
	size_t GetCapacity() const noexcept { return p_ ? header()->cap : 0; }
	const uint8_t* get() const noexcept { return p_; }

protected:
	uint8_t* alloc(size_t cap);
	void release() noexcept {
		if (p_) {
			if (auto& hdr = *header(); hdr.refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
				hdr.~dataHeader();
				operator delete(p_);
			}
			p_ = nullptr;
		}
	}

	dataHeader* header() noexcept { return reinterpret_cast<dataHeader*>(p_); }
	const dataHeader* header() const noexcept { return reinterpret_cast<dataHeader*>(p_); }
	friend std::ostream& operator<<(std::ostream& os, const PayloadValue&);
	// Data of elements, shared
	uint8_t* p_;
};

}  // namespace reindexer
