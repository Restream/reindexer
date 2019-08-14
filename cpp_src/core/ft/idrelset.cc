
#include "idrelset.h"
#include <algorithm>
#include "estl/h_vector.h"
#include "sort/pdqsort.hpp"
#include "tools/varint.h"

namespace reindexer {

size_t IdRelType::pack(uint8_t* buf) const {
	auto p = buf;
	p += uint32_pack(id, p);
	p += uint32_pack(pos.size(), p);
	uint32_t last = 0;
	for (auto c : pos) {
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
	id = parse_uint32(l, p);
	p += l, len -= l;

	l = scan_varint(len, p);
	assert(l != 0);
	int sz = parse_uint32(l, p);
	p += l, len -= l;

	pos.resize(sz);
	uint32_t last = 0;
	for (int i = 0; i < sz; i++) {
		l = scan_varint(len, p);
		assert(l != 0);
		pos[i].fpos = parse_uint32(l, p) + last;
		last = pos[i].fpos;
		p += l, len -= l;
	}

	return p - buf;
}

int IdRelType::distance(const IdRelType& other, int max) const {
	for (auto i = pos.begin(), j = other.pos.begin(); i != pos.end() && j != other.pos.end();) {
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
int IdRelType::wordsInField(int field) {
	unsigned i = 0;
	int wcount = 0;
	// TODO: optiminize here, binary search or precalculate
	while (i < pos.size() && pos[i].field() < field) i++;
	while (i < pos.size() && pos[i].field() == field) i++, wcount++;

	return wcount;
}

int IdRelSet::Add(VDocIdType id, int pos, int field) {
	if (id > max_id_) max_id_ = id;
	if (id < min_id_) min_id_ = id;

	if (!size() || back().id != id) {
		IdRelType idrel;
		idrel.id = id;
		push_back(std::move(idrel));
	}
	back().pos.push_back({pos, field});
	return back().pos.size();
}

void IdRelSet::Commit() {
	boost::sort::pdqsort(begin(), end(), [](const IdRelType& lhs, const IdRelType& rhs) { return lhs.rank() > rhs.rank(); });
}

void IdRelSet::SimpleCommit() {
	for (auto& val : *this) {
		boost::sort::pdqsort(val.pos.begin(), val.pos.end(),
							 [](const IdRelType::PosType& lhs, const IdRelType::PosType& rhs) { return lhs.pos() < rhs.pos(); });
	}
}

}  // namespace reindexer
