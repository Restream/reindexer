#pragma once

#include <stddef.h>
#include <atomic>
#include <cstdint>
#include <iosfwd>
#include "tools/lsn.h"

#ifdef RX_WITH_STDLIB_DEBUG
#include "tools/assertrx.h"
#endif	// RX_WITH_STDLIB_DEBUG

namespace reindexer {

// The full item's payload object. It must be speed & size optimized.
// On 64-bit platforms the pointer is stored as two uint32_t so alignof is 4 and
// std::pair<PayloadValue, KeyEntryPK> packs to 12 bytes instead of 16.
class [[nodiscard]] PayloadValue {
public:
	typedef std::atomic<int32_t> refcounter;
	static constexpr bool kHasCompressedPtrStorage =
#if UINTPTR_MAX > UINT32_MAX
		true;
#else	// UINTPTR_MAX <= UINT32_MAX
		false;
#endif	// UINTPTR_MAX > UINT32_MAX

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

	PayloadValue() noexcept = default;
	PayloadValue(const PayloadValue& other) noexcept {
		setPtr(other.ptr());
		if (auto* p = ptr()) {
			header(p)->refcount.fetch_add(1, std::memory_order_relaxed);
		}
	}
	// Alloc payload store with size, and copy data from another array
	PayloadValue(size_t size, const uint8_t* ptr = nullptr, size_t cap = 0);
	~PayloadValue() { release(); }
	PayloadValue& operator=(const PayloadValue& other) noexcept {
		if (&other != this) {
			release();
			setPtr(other.ptr());
			if (auto* p = ptr()) {
				header(p)->refcount.fetch_add(1, std::memory_order_relaxed);
			}
		}
		return *this;
	}
	PayloadValue(PayloadValue&& other) noexcept {
		setPtr(other.ptr());
		other.setPtr(nullptr);
	}
	PayloadValue& operator=(PayloadValue&& other) noexcept {
		if (&other != this) {
			release();
			setPtr(other.ptr());
			other.setPtr(nullptr);
		}

		return *this;
	}

	// Clone if data is shared for copy-on-write.
	void Clone(size_t size = 0);
	// Resize
	void Resize(size_t oldSize, size_t newSize);
	// Get data pointer
	uint8_t* Ptr() const noexcept { return ptr() + sizeof(dataHeader); }
	void SetLSN(lsn_t lsn) noexcept { header()->lsn = lsn; }
	lsn_t GetLSN() const noexcept { return ptr() ? header()->lsn : lsn_t(); }
	bool IsFree() const noexcept { return ptr() == nullptr; }
	void Free() noexcept { release(); }
	size_t GetCapacity() const noexcept { return ptr() ? header()->cap : 0; }
	const uint8_t* get() const noexcept { return ptr(); }

protected:
	struct [[nodiscard]] PtrStorage {
		RX_ALWAYS_INLINE uint8_t* get() const noexcept {
#if UINTPTR_MAX > UINT32_MAX
			return reinterpret_cast<uint8_t*>((uintptr_t(pHi_) << 32) | uintptr_t(pLo_));
#else	// UINTPTR_MAX <= UINT32_MAX
			return p_;
#endif	// UINTPTR_MAX > UINT32_MAX
		}
		RX_ALWAYS_INLINE void set(uint8_t* p) noexcept {
#if UINTPTR_MAX > UINT32_MAX
			const auto u = reinterpret_cast<uintptr_t>(p);
			pLo_ = uint32_t(u);
			pHi_ = uint32_t(u >> 32);
#else	// UINTPTR_MAX <= UINT32_MAX
			p_ = p;
#endif	// UINTPTR_MAX > UINT32_MAX
		}

#if UINTPTR_MAX > UINT32_MAX
		uint32_t pLo_ = 0;
		uint32_t pHi_ = 0;
#else	// UINTPTR_MAX <= UINT32_MAX
		uint8_t* p_ = nullptr;
#endif	// UINTPTR_MAX > UINT32_MAX
	};

	uint8_t* alloc(size_t cap);
	void release() noexcept {
		if (auto* p = ptr()) {
			if (auto& hdr = *header(p); hdr.refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
				hdr.~dataHeader();
				operator delete(p);
			}
			setPtr(nullptr);
		}
	}

	RX_ALWAYS_INLINE uint8_t* ptr() const noexcept { return ptr_.get(); }
	RX_ALWAYS_INLINE void setPtr(uint8_t* p) noexcept { ptr_.set(p); }

	dataHeader* header() noexcept { return header(ptr()); }
	const dataHeader* header() const noexcept { return header(ptr()); }
	static dataHeader* header(uint8_t* p) noexcept { return reinterpret_cast<dataHeader*>(p); }
	static const dataHeader* header(const uint8_t* p) noexcept { return reinterpret_cast<const dataHeader*>(p); }
	friend std::ostream& operator<<(std::ostream& os, const PayloadValue&);

	PtrStorage ptr_;
};

static_assert(sizeof(PayloadValue) == sizeof(void*));
static_assert(!PayloadValue::kHasCompressedPtrStorage || alignof(PayloadValue) == 4);

}  // namespace reindexer
