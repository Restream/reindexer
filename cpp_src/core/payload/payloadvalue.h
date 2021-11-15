#pragma once

#include <assert.h>
#include <stddef.h>
#include <atomic>
#include "tools/lsn.h"

namespace reindexer {

// The full item's payload object. It must be speed & size optimized
class PayloadValue {
public:
	typedef std::atomic<int32_t> refcounter;
	struct dataHeader {
		dataHeader() : refcount(1), cap(0), lsn(-1) {}

		~dataHeader() { assert(refcount.load() == 0); }
		refcounter refcount;
		unsigned cap;
		lsn_t lsn;
	};

	PayloadValue() : p_(nullptr) {}
	PayloadValue(const PayloadValue &);
	// Alloc payload store with size, and copy data from another array
	PayloadValue(size_t size, const uint8_t *ptr = nullptr, size_t cap = 0);
	~PayloadValue();
	PayloadValue &operator=(const PayloadValue &other) {
		if (&other != this) {
			release();
			p_ = other.p_;
			if (p_) header()->refcount.fetch_add(1, std::memory_order_relaxed);
		}
		return *this;
	}
	PayloadValue(PayloadValue &&other) noexcept : p_(other.p_) { other.p_ = nullptr; }
	PayloadValue &operator=(PayloadValue &&other) noexcept {
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
	uint8_t *Ptr() const { return p_ + sizeof(dataHeader); }
	void SetLSN(lsn_t lsn) { header()->lsn = lsn; }
	lsn_t GetLSN() const { return p_ ? header()->lsn : lsn_t(); }
	bool IsFree() const { return bool(p_ == nullptr); }
	void Free() { release(); }
	size_t GetCapacity() const { return header()->cap; }
	const uint8_t *get() const { return p_; }

protected:
	uint8_t *alloc(size_t cap);
	void release();

	dataHeader *header() { return reinterpret_cast<dataHeader *>(p_); }
	const dataHeader *header() const { return reinterpret_cast<dataHeader *>(p_); }
	// Data of elements, shared
	uint8_t *p_;
};

}  // namespace reindexer
