#include "core/payloaddata.h"
#include "string.h"
#include "tools/errors.h"

namespace reindexer {

PayloadData::PayloadData(size_t size, const uint8_t *ptr, size_t cap) : p_(nullptr) {
	p_ = alloc((cap != 0) ? cap : size);

	if (ptr)
		memcpy(Ptr(), ptr, size);
	else
		memset(Ptr(), 0, size);
}

PayloadData::PayloadData(const PayloadData &other) : p_(other.p_) {
	if (p_) {
		header()->refcount.fetch_add(1);
	}
}

PayloadData::~PayloadData() { release(); }

uint8_t *PayloadData::alloc(size_t cap) {
	auto pn = (uint8_t *)operator new(cap + sizeof(dataHeader));
	dataHeader *nheader = (dataHeader *)pn;
	new (nheader) dataHeader();
	nheader->cap = cap;
	if (p_) {
		nheader->updated = header()->updated;
		nheader->version = header()->version;
	}

	return pn;
}

void PayloadData::release() {
	if (p_ && header()->refcount.fetch_sub(1) == 1) {
		header()->~dataHeader();
		delete p_;
	}
	p_ = nullptr;
}

void PayloadData::AllocOrClone(size_t size) {
	// If we have exclusive data - just up version
	if (p_ && header()->refcount.load() == 1) {
		header()->version++;
		return;
	}

	auto pn = alloc(p_ ? header()->cap : size);
	if (p_) {
		// Make new data & copy
		memcpy(pn + sizeof(dataHeader), Ptr(), header()->cap);
		// Release old data
		release();
	}

	p_ = pn;
	header()->version++;
}

void PayloadData::Resize(size_t oldSize, size_t newSize) {
	assert(p_);
	assert(header()->refcount.load() == 1);

	if (newSize <= header()->cap) return;

	auto pn = alloc(newSize);
	memcpy(pn + sizeof(dataHeader), Ptr(), oldSize);

	// Release old data
	release();
	p_ = pn;
}

}  // namespace reindexer
