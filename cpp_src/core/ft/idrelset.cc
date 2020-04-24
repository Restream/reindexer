
#include "idrelset.h"
#include <algorithm>
#include "estl/h_vector.h"
#include "sort/pdqsort.hpp"
#include "tools/varint.h"

namespace reindexer {

size_t IdRelType::pack(uint8_t* buf) const {
	auto p = buf;
	p += uint32_pack(id_, p);
	p += uint32_pack(pos_.size(), p);
	uint32_t last = 0;
	for (auto c : pos_) {
		p += uint32_pack(c.fpos - last, p);
		last = c.fpos;
	}
	return p - buf;
}

size_t IdRelType::unpack(const uint8_t* buf, unsigned len) {
	auto p = buf;
	assert(len != 0);
	auto l = scan_varint(len, p);
	assert(l != 0);
	id_ = parse_uint32(l, p);
	p += l, len -= l;

	l = scan_varint(len, p);
	assert(l != 0);
	int sz = parse_uint32(l, p);
	p += l, len -= l;

	pos_.resize(sz);
	usedFieldsMask_ = 0;
	uint32_t last = 0;
	for (int i = 0; i < sz; i++) {
		l = scan_varint(len, p);
		assert(l != 0);
		pos_[i].fpos = parse_uint32(l, p) + last;
		last = pos_[i].fpos;
		addField(pos_[i].field());
		p += l, len -= l;
	}

	return p - buf;
}

int IdRelType::Distance(const IdRelType& other, int max) const {
	for (auto i = pos_.begin(), j = other.pos_.begin(); i != pos_.end() && j != other.pos_.end();) {
		bool sign = i->fpos > j->fpos;
		int cur = sign ? i->fpos - j->fpos : j->fpos - i->fpos;
		if (cur < max && cur < (1 << PosType::posBits)) {
			max = cur;
			if (max <= 1) break;
		}
		(sign) ? j++ : i++;
	}
	return max;
}
int IdRelType::WordsInField(int field) {
	unsigned i = 0;
	int wcount = 0;
	// TODO: optiminize here, binary search or precalculate
	while (i < pos_.size() && pos_[i].field() < field) i++;
	while (i < pos_.size() && pos_[i].field() == field) i++, wcount++;

	return wcount;
}

int IdRelSet::Add(VDocIdType id, int pos, int field) {
	if (id > max_id_) max_id_ = id;
	if (id < min_id_) min_id_ = id;

	if (!size() || back().Id() != id) {
		emplace_back(id);
	}
	back().Add(pos, field);
	return back().Size();
}

void IdRelType::SimpleCommit() {
	boost::sort::pdqsort(pos_.begin(), pos_.end(),
						 [](const IdRelType::PosType& lhs, const IdRelType::PosType& rhs) { return lhs.pos() < rhs.pos(); });
}

}  // namespace reindexer
