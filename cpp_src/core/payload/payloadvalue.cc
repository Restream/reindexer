#include "payloadvalue.h"
#include <iostream>
#include "core/keyvalue/p_string.h"

namespace reindexer {

PayloadValue::PayloadValue(size_t size, const uint8_t* ptr, size_t cap) : p_(nullptr) {
	p_ = alloc((cap != 0) ? cap : size);

	if (ptr) {
		memcpy(Ptr(), ptr, size);
	} else {
		memset(Ptr(), 0, size);
	}
}

uint8_t* PayloadValue::alloc(size_t cap) {
	auto pn = reinterpret_cast<uint8_t*>(operator new(cap + sizeof(dataHeader)));
	dataHeader* nheader = reinterpret_cast<dataHeader*>(pn);
	new (nheader) dataHeader();
	nheader->cap = cap;
	if (p_) {
		nheader->lsn = header()->lsn;
	} else {
		nheader->lsn = lsn_t();
	}
	return pn;
}

void PayloadValue::Clone(size_t size) {
	// If we have exclusive data - just up lsn
	if (p_ && header()->refcount.load(std::memory_order_acquire) == 1) {
		return;
	}
	assertrx(size || p_);

	auto pn = alloc(p_ ? header()->cap : size);
	if (p_) {
		// Make new data & copy
		memcpy(pn + sizeof(dataHeader), Ptr(), header()->cap);
		// Release old data
		release();
	} else {
		memset(pn + sizeof(dataHeader), 0, size);
	}

	p_ = pn;
}

void PayloadValue::Resize(size_t oldSize, size_t newSize) {
	assertrx(p_);
	assertrx(header()->refcount.load(std::memory_order_acquire) == 1);

	if (newSize <= header()->cap) {
		return;
	}

	auto pn = alloc(newSize);
	memcpy(pn + sizeof(dataHeader), Ptr(), oldSize);
	memset(pn + sizeof(dataHeader) + oldSize, 0, newSize - oldSize);

	// Release old data
	release();
	p_ = pn;
}

std::ostream& operator<<(std::ostream& os, const PayloadValue& pv) {
	os << "{p_: " << std::hex << static_cast<const void*>(pv.p_) << std::dec;
	if (pv.p_) {
		const auto* header = pv.header();
		os << ", refcount: " << header->refcount.load(std::memory_order_relaxed) << ", cap: " << header->cap << ", lsn: " << header->lsn
		   << ", [" << std::hex;
		const uint8_t* ptr = pv.Ptr();
		const size_t cap = header->cap;
		for (size_t i = 0; i < cap; ++i) {
			if (i != 0) {
				os << ' ';
			}
			os << static_cast<unsigned>(ptr[i]);
		}
		os << std::dec << "], tuple: ";
		assertrx(cap >= sizeof(p_string));
		const p_string& str = *reinterpret_cast<const p_string*>(ptr);
		str.Dump(os);
	}
	return os << '}';
}

}  // namespace reindexer
