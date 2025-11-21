#include "idrelset.h"
#include <algorithm>
#include "estl/h_vector.h"
#include "tools/varint.h"

namespace reindexer {

size_t IdRelType::pack(uint8_t* buf, VDocIdType previousId, uint32_t previousField) const {
	auto p = buf;

	bool idModified = false;
	if (id_ >= previousId) {
		p += uint32_pack(id_ - previousId, p);
		idModified = true;
	} else {
		p += uint32_pack(id_, p);
	}

	assertrx_dbg(pos_.size() > 0);

	uint32_t field = pos_[0].field();
	uint32_t shift = pos_[0].pos();
	uint32_t arrayIdx = pos_[0].arrayIdx();

	bool fieldIsSame = (field == previousField);
	bool arrayIDxIsZero = (arrayIdx == 0);
	bool sizeIs1 = (pos_.size() == 1);

	shift = (shift << 4) | idModified | (fieldIsSame << 1) | (sizeIs1 << 2) | (arrayIDxIsZero << 3);

	p += uint32_pack(shift, p);
	if (!fieldIsSame) {
		p += uint32_pack(field, p);
	}
	if (!arrayIDxIsZero) {
		p += uint32_pack(arrayIdx - 1, p);
	}
	if (!sizeIs1) {
		p += uint32_pack(pos_.size() - 1, p);
	}

	shift = pos_[0].pos();
	for (size_t i = 1; i < pos_.size(); i++) {
		uint32_t posField = pos_[i].field();
		uint32_t posArrayIdx = pos_[i].arrayIdx();
		uint32_t posShift = pos_[i].pos();

		bool fieldIsSame = (posField == field);
		bool arrayIdxIsSame = (posArrayIdx == arrayIdx);
		uint32_t shiftToSave = (fieldIsSame && arrayIdxIsSame) ? posShift - shift : posShift;
		uint32_t arrayIdxToSave = (fieldIsSame) ? posArrayIdx - arrayIdx : posArrayIdx;

		shiftToSave = (shiftToSave << 2) | (fieldIsSame) | (arrayIdxIsSame << 1);
		p += uint32_pack(shiftToSave, p);
		if (!fieldIsSame) {
			p += uint32_pack(posField - field, p);
		}
		if (!arrayIdxIsSame) {
			p += uint32_pack(arrayIdxToSave, p);
		}

		field = posField;
		arrayIdx = posArrayIdx;
		shift = posShift;
	}

	return p - buf;
}

inline uint32_t get_uint32(const uint8_t** data, uint32_t* len) {
	auto l = scan_varint(*len, *data);
	assertrx_dbg(l > 0 && l <= 5);
	uint32_t res = parse_uint32(l, *data);
	*data += l;
	*len -= l;
	return res;
}

size_t IdRelType::unpack(const uint8_t* buf, uint32_t len, VDocIdType previousId, uint32_t previousField) {
	auto data = buf;
	id_ = get_uint32(&data, &len);

	uint32_t lastPosShift = get_uint32(&data, &len);
	bool idModified = (lastPosShift & 1);
	bool fieldIsSame = (lastPosShift & 2);
	bool sizeIs1 = (lastPosShift & 4);
	bool arrayIDxIsZero = (lastPosShift & 8);
	lastPosShift = lastPosShift >> 4;

	if (idModified) {
		id_ += previousId;
	}
	uint32_t lastPosField = previousField;
	if (!fieldIsSame) {
		lastPosField = get_uint32(&data, &len);
	}
	uint32_t lastPosArrayIdx = 0;
	if (!arrayIDxIsZero) {
		lastPosArrayIdx = get_uint32(&data, &len) + 1;
	}

	uint32_t size = 1;
	if (!sizeIs1) {
		size = get_uint32(&data, &len) + 1;
	}

	pos_.resize(size);
	pos_[0] = PosType(lastPosShift, lastPosField, lastPosArrayIdx);

	for (uint32_t i = 1; i < size; i++) {
		uint32_t nextShift = get_uint32(&data, &len);
		uint32_t nextField = lastPosField;
		uint32_t nextPosArrayIdx = lastPosArrayIdx;
		bool fieldIsSame = (nextShift & 1);
		bool arrayIdxIsSame = (nextShift & 2);
		nextShift = nextShift >> 2;

		if (fieldIsSame && arrayIdxIsSame) {
			nextShift += lastPosShift;
		}

		if (!fieldIsSame) {
			nextField = get_uint32(&data, &len) + lastPosField;
		}

		if (!arrayIdxIsSame) {
			nextPosArrayIdx = get_uint32(&data, &len);
			nextPosArrayIdx += fieldIsSame ? lastPosArrayIdx : 0;
		}

		pos_[i] = PosType(nextShift, nextField, nextPosArrayIdx);

		lastPosShift = nextShift;
		lastPosField = nextField;
		lastPosArrayIdx = nextPosArrayIdx;
	}

	return data - buf;
}

size_t IdRelType::packWithoutArrayIdxs(uint8_t* buf, VDocIdType previousId, uint32_t previousField) const {
	auto p = buf;

	bool idModified = false;
	if (id_ >= previousId) {
		p += uint32_pack(id_ - previousId, p);
		idModified = true;
	} else {
		p += uint32_pack(id_, p);
	}

	assertrx_dbg(pos_.size() > 0);

	uint32_t lastPosField = pos_[0].field();
	uint32_t lastPosShift = pos_[0].pos();

	bool fieldIsSame = (lastPosField == previousField);
	bool sizeIs1 = (pos_.size() == 1);

	lastPosShift = (lastPosShift << 3) | idModified | (fieldIsSame << 1) | (sizeIs1 << 2);

	p += uint32_pack(lastPosShift, p);
	if (!fieldIsSame) {
		p += uint32_pack(lastPosField, p);
	}
	if (!sizeIs1) {
		p += uint32_pack(pos_.size() - 1, p);
	}

	lastPosShift = pos_[0].pos();
	for (size_t i = 1; i < pos_.size(); i++) {
		uint32_t posField = pos_[i].field();
		uint32_t posShift = pos_[i].pos();

		bool fieldIsSame = (posField == lastPosField);
		uint32_t shiftToSave = fieldIsSame ? posShift - lastPosShift : posShift;

		shiftToSave = (shiftToSave << 1) | fieldIsSame;
		p += uint32_pack(shiftToSave, p);
		if (!fieldIsSame) {
			p += uint32_pack(posField - lastPosField, p);
		}

		lastPosField = posField;
		lastPosShift = posShift;
	}

	return p - buf;
}

size_t IdRelType::unpackWithoutArrayIdxs(const uint8_t* buf, uint32_t len, VDocIdType previousId, uint32_t previousField) {
	auto data = buf;
	id_ = get_uint32(&data, &len);

	uint32_t lastPosShift = get_uint32(&data, &len);
	bool idModified = (lastPosShift & 1);
	bool fieldIsSame = (lastPosShift & 2);
	bool sizeIs1 = (lastPosShift & 4);
	lastPosShift = lastPosShift >> 3;

	if (idModified) {
		id_ += previousId;
	}
	uint32_t lastPosField = previousField;
	if (!fieldIsSame) {
		lastPosField = get_uint32(&data, &len);
	}
	uint32_t size = 1;
	if (!sizeIs1) {
		size = get_uint32(&data, &len) + 1;
	}

	pos_.resize(size);
	pos_[0] = PosType(lastPosShift, lastPosField, 0);

	for (uint32_t i = 1; i < size; i++) {
		uint32_t nextShift = get_uint32(&data, &len);
		uint32_t nextField = lastPosField;
		bool fieldIsSame = (nextShift & 1);
		nextShift = nextShift >> 1;

		if (fieldIsSame) {
			nextShift += lastPosShift;
		} else {
			nextField = get_uint32(&data, &len) + lastPosField;
		}

		pos_[i] = PosType(nextShift, nextField, 0);

		lastPosShift = nextShift;
		lastPosField = nextField;
	}

	return data - buf;
}

}  // namespace reindexer
