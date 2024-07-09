#include "idrelset.h"
#include <algorithm>
#include "estl/h_vector.h"
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
	assertrx_dbg(len != 0);
	auto l = scan_varint(len, p);
	assertrx_dbg(l != 0);
	id_ = parse_uint32(l, p);
	p += l, len -= l;

	l = scan_varint(len, p);
	assertrx_dbg(l != 0);
	int sz = parse_uint32(l, p);
	p += l, len -= l;

	pos_.resize(sz);
	usedFieldsMask_ = 0;
	uint32_t last = 0;
	for (int i = 0; i < sz; i++) {
		l = scan_varint(len, p);
		assertrx_dbg(l != 0);
		pos_[i].fpos = parse_uint32(l, p) + last;
		last = pos_[i].fpos;
		addField(pos_[i].field());
		p += l, len -= l;
	}

	return p - buf;
}

int IdRelType::Distance(const IdRelType& other, int max) const {
	for (auto i = pos_.begin(), j = other.pos_.begin(); i != pos_.end() && j != other.pos_.end();) {
		// fpos - field number + word position in the field
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
int IdRelType::WordsInField(int field) const noexcept {
	const auto lower = std::lower_bound(pos_.cbegin(), pos_.cend(), field, [](PosType p, int f) { return p.field() < f; });
	return std::upper_bound(lower, pos_.cend(), field, [](int f, PosType p) { return f < p.field(); }) - lower;
}
int IdRelType::MinPositionInField(int field) const noexcept {
	auto lower = std::lower_bound(pos_.cbegin(), pos_.cend(), field, [](PosType p, int f) { return p.field() < f; });
	assertrx(lower != pos_.cend() && lower->field() == field);
	int res = lower->pos();
	while (++lower != pos_.cend() && lower->field() == field) {
		if (lower->pos() < res) res = lower->pos();
	}
	return res;
}

}  // namespace reindexer
