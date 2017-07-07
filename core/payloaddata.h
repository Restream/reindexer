#pragma once

#include <assert.h>
#include <stdint.h>
#include <atomic>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <vector>

namespace reindexer {

// The full item's payload object. It must be speed & size optimized
extern std::atomic<int32_t> payload_cnt;
class PayloadData {
public:
	typedef std::atomic<int16_t> refcounter;
	struct dataHeader {
		dataHeader() : refcount(1), version(1), updated(0), cap(0) { payload_cnt.fetch_add(1); }

		~dataHeader() {
			assert(refcount.load() == 0);
			payload_cnt.fetch_sub(1);
		}
		refcounter refcount;
		int16_t version : 15;
		int16_t updated : 1;
		unsigned cap;
	};

	PayloadData() : p_(nullptr) {}
	PayloadData(const PayloadData &);
	// Alloc payload store with size, and copy data from another array
	PayloadData(size_t size, const uint8_t *ptr = nullptr, size_t cap = 0);
	~PayloadData();
	PayloadData &operator=(const PayloadData &other) {
		if (&other != this) {
			release();
			p_ = other.p_;
			if (p_) header()->refcount.fetch_add(1);
		}
		return *this;
	}

	// Clone if data is shared for copy-on-write.
	void AllocOrClone(size_t size);
	// Resize
	void Resize(size_t oldSize, size_t newSize);
	// Get data pointer
	uint8_t *Ptr() const { return p_ + sizeof(dataHeader); }
	void SetVersion(int version) { header()->version = static_cast<int16_t>(version); }
	int GetVersion() const { return header()->version; }
	void SetUpdated(bool updated) { header()->updated = updated; }
	bool IsUpdated() const { return bool(p_ == nullptr || header()->updated); }
	bool IsFree() const { return bool(p_ == nullptr); }
	void Free() { release(); }

protected:
	uint8_t *alloc(size_t cap);
	void release();

	dataHeader *header() { return reinterpret_cast<dataHeader *>(p_); }
	const dataHeader *header() const { return reinterpret_cast<dataHeader *>(p_); }
	// Data of elements, shared
	uint8_t *p_;
};

}  // namespace reindexer
