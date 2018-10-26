#include "payloadvalue.h"
#include <chrono>
#include "string.h"
#include "tools/errors.h"
namespace reindexer {

PayloadValue::PayloadValue(size_t size, const uint8_t *ptr, size_t cap) : p_(nullptr) {
	p_ = alloc((cap != 0) ? cap : size);

	if (ptr)
		memcpy(Ptr(), ptr, size);
	else
		memset(Ptr(), 0, size);
}

PayloadValue::PayloadValue(const PayloadValue &other) : p_(other.p_) {
	if (p_) {
		header()->refcount.fetch_add(1);
	}
}

PayloadValue::~PayloadValue() { release(); }

uint8_t *PayloadValue::alloc(size_t cap) {
	auto pn = reinterpret_cast<uint8_t *>(operator new(cap + sizeof(dataHeader)));
	dataHeader *nheader = reinterpret_cast<dataHeader *>(pn);
	new (nheader) dataHeader();
	nheader->cap = cap;
	if (p_) {
		nheader->lsn = header()->lsn;
	}

	return pn;
}

void PayloadValue::release() {
	if (p_ && header()->refcount.fetch_sub(1) == 1) {
		header()->~dataHeader();
		delete p_;
	}
	p_ = nullptr;
}

void PayloadValue::Clone(size_t size) {
	// If we have exclusive data - just up lsn
	if (p_ && header()->refcount.load() == 1) {
		return;
	}
	assert(size || p_);

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
	assert(p_);
	assert(header()->refcount.load() == 1);

	if (newSize <= header()->cap) return;

	auto pn = alloc(newSize);
	memcpy(pn + sizeof(dataHeader), Ptr(), oldSize);
	memset(pn + sizeof(dataHeader) + oldSize, 0, newSize - oldSize);

	// Release old data
	release();
	p_ = pn;
}

}  // namespace reindexer
