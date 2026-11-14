#include "payloadvalue.h"
#include <iostream>
#include "core/keyvalue/p_string.h"

namespace reindexer {

PayloadValue::PayloadValue(size_t size, const uint8_t* data, size_t cap) {
	setPtr(alloc((cap != 0) ? cap : size));

	if (data) {
		memcpy(Ptr(), data, size);
	} else {
		memset(Ptr(), 0, size);
	}
}

uint8_t* PayloadValue::alloc(size_t cap) {
	auto pn = reinterpret_cast<uint8_t*>(operator new(cap + sizeof(dataHeader)));
	dataHeader* nheader = header(pn);
	new (nheader) dataHeader();
	nheader->cap = cap;
	if (auto* p = ptr()) {
		nheader->lsn = header(p)->lsn;
	} else {
		nheader->lsn = lsn_t();
	}
	return pn;
}

void PayloadValue::Clone(size_t size) {
	// If we have exclusive data - just up lsn
	if (auto* p = ptr(); p && header(p)->refcount.load(std::memory_order_acquire) == 1) {
		return;
	}
	assertrx(size || ptr());

	auto pn = alloc(ptr() ? header()->cap : size);
	if (ptr()) {
		// Make new data & copy
		memcpy(pn + sizeof(dataHeader), Ptr(), header()->cap);
		// Release old data
		release();
	} else {
		memset(pn + sizeof(dataHeader), 0, size);
	}

	setPtr(pn);
}

void PayloadValue::Resize(size_t oldSize, size_t newSize) {
	assertrx(ptr());
	assertrx(header()->refcount.load(std::memory_order_acquire) == 1);

	if (newSize <= header()->cap) {
		return;
	}

	auto pn = alloc(newSize);
	memcpy(pn + sizeof(dataHeader), Ptr(), oldSize);
	memset(pn + sizeof(dataHeader) + oldSize, 0, newSize - oldSize);

	// Release old data
	release();
	setPtr(pn);
}

std::ostream& operator<<(std::ostream& os, const PayloadValue& pv) {
	os << "{p_: " << std::hex << static_cast<const void*>(pv.ptr()) << std::dec;
	if (auto* p = pv.ptr()) {
		const auto* hdr = PayloadValue::header(p);
		os << ", refcount: " << hdr->refcount.load(std::memory_order_relaxed) << ", cap: " << hdr->cap << ", lsn: " << hdr->lsn << ", ["
		   << std::hex;
		const uint8_t* data = pv.Ptr();
		const size_t cap = hdr->cap;
		for (size_t i = 0; i < cap; ++i) {
			if (i != 0) {
				os << ' ';
			}
			os << static_cast<unsigned>(data[i]);
		}
		os << std::dec << "], tuple: ";
		assertrx(cap >= sizeof(p_string));
		const p_string& str = *reinterpret_cast<const p_string*>(data);
		str.Dump(os);
	}
	return os << '}';
}

}  // namespace reindexer
