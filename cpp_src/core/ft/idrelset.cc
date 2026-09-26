#include "idrelset.h"
#include <algorithm>
#include <tuple>
#include "tools/assertrx.h"

namespace reindexer {

namespace {

enum [[nodiscard]] ControlBits : uint8_t {
	kIdDeltaBit = 1u << 0,
	kIdLenShift = 1,
	kIdLenMask = 0x3u,
	kVersionShift = 3,
	kVersionMask = 0x7u,
	kVersionInlineBit = 1u << 6,
	kComplexBit = 1u << 7,
};

// Complex records: after occurrence count, store hits payload length in
// 1 byte when count < kShortPayloadLenCount, otherwise in 4 bytes.
constexpr uint32_t kShortPayloadLenCount = 8;

inline unsigned byteLen32(uint32_t v) noexcept {
	if (v <= 0xffu) {
		return 1;
	}
	if (v <= 0xffffu) {
		return 2;
	}
	if (v <= 0xffffffu) {
		return 3;
	}
	return 4;
}

// Native little-endian load/store without memcpy (hot path for packed postings).
inline void writeBytes(uint8_t* p, uint64_t v, unsigned n) noexcept {
	switch (n) {
		case 8:
			p[7] = uint8_t(v >> 56);
			[[fallthrough]];
		case 7:
			p[6] = uint8_t(v >> 48);
			[[fallthrough]];
		case 6:
			p[5] = uint8_t(v >> 40);
			[[fallthrough]];
		case 5:
			p[4] = uint8_t(v >> 32);
			[[fallthrough]];
		case 4:
			p[3] = uint8_t(v >> 24);
			[[fallthrough]];
		case 3:
			p[2] = uint8_t(v >> 16);
			[[fallthrough]];
		case 2:
			p[1] = uint8_t(v >> 8);
			[[fallthrough]];
		case 1:
			p[0] = uint8_t(v);
			break;
		default:
			assertrx_dbg(false);
			break;
	}
}

inline uint64_t readBytes(const uint8_t* p, unsigned n) noexcept {
	switch (n) {
		case 1:
			return p[0];
		case 2:
			return uint64_t(p[0]) | (uint64_t(p[1]) << 8);
		case 3:
			return uint64_t(p[0]) | (uint64_t(p[1]) << 8) | (uint64_t(p[2]) << 16);
		case 4:
			return uint64_t(p[0]) | (uint64_t(p[1]) << 8) | (uint64_t(p[2]) << 16) | (uint64_t(p[3]) << 24);
		case 5:
			return uint64_t(p[0]) | (uint64_t(p[1]) << 8) | (uint64_t(p[2]) << 16) | (uint64_t(p[3]) << 24) | (uint64_t(p[4]) << 32);
		case 6:
			return uint64_t(p[0]) | (uint64_t(p[1]) << 8) | (uint64_t(p[2]) << 16) | (uint64_t(p[3]) << 24) | (uint64_t(p[4]) << 32) |
				   (uint64_t(p[5]) << 40);
		case 7:
			return uint64_t(p[0]) | (uint64_t(p[1]) << 8) | (uint64_t(p[2]) << 16) | (uint64_t(p[3]) << 24) | (uint64_t(p[4]) << 32) |
				   (uint64_t(p[5]) << 40) | (uint64_t(p[6]) << 48);
		case 8:
			return uint64_t(p[0]) | (uint64_t(p[1]) << 8) | (uint64_t(p[2]) << 16) | (uint64_t(p[3]) << 24) | (uint64_t(p[4]) << 32) |
				   (uint64_t(p[5]) << 40) | (uint64_t(p[6]) << 48) | (uint64_t(p[7]) << 56);
		default:
			assertrx_dbg(false);
			return 0;
	}
}

inline void writeU16(uint8_t* p, uint16_t v) noexcept {
	p[0] = uint8_t(v);
	p[1] = uint8_t(v >> 8);
}

inline uint16_t readU16(const uint8_t* p) noexcept { return uint16_t(p[0]) | (uint16_t(p[1]) << 8); }

// [2:len][6:payload]: len=0 → value in low 6 bits; len=1/2/3 → 1/2/4 following bytes (covers full uint32)
inline uint8_t* packLen6(uint8_t* p, uint32_t value) noexcept {
	if (value <= 0x3fu) {
		*p++ = uint8_t(value);
		return p;
	}
	if (value <= 0xffu) {
		*p++ = uint8_t(1u << 6);
		writeBytes(p, value, 1);
		return p + 1;
	}
	if (value <= 0xffffu) {
		*p++ = uint8_t(2u << 6);
		writeBytes(p, value, 2);
		return p + 2;
	}
	*p++ = uint8_t(3u << 6);
	writeBytes(p, value, 4);
	return p + 4;
}

inline const uint8_t* unpackLen6(const uint8_t* p, [[maybe_unused]] const uint8_t* end, uint32_t& value) {
	assertrx_dbg(p < end);
	const uint8_t hdr = *p++;
	const unsigned lenCode = hdr >> 6;
	if (lenCode == 0) {
		value = hdr & 0x3fu;
		return p;
	}
	static constexpr unsigned kBytesByCode[4] = {0, 1, 2, 4};
	const unsigned n = kBytesByCode[lenCode];
	assertrx_dbg(size_t(end - p) >= n);
	value = uint32_t(readBytes(p, n));
	return p + n;
}

inline bool canPackSimple(const PositionsVector& pos, unsigned fieldBits) noexcept {
	if (pos.size() != 1) {
		return false;
	}
	const auto& p = pos[0];
	// Non-zero arrayIdx is allowed when it fits in one byte; presence of the byte itself is
	// controlled by PackedIdRelVec::arrayFoundPos_ (storeArrayIdx).
	if (p.arrayIdx() > 0xffu) {
		return false;
	}
	const unsigned posBits = 16u - fieldBits;
	if (fieldBits == 0) {
		return p.field() == 0 && p.pos() <= 0xffffu;
	}
	if (p.field() >= (1u << fieldBits)) {
		return false;
	}
	return p.pos() < (1u << posBits);
}

}  // namespace

size_t IdRelType::pack(uint8_t* buf, uint32_t previousVdocId, unsigned fieldBits, bool storeArrayIdx) const {
	auto* p = buf;
	assertrx_dbg(pos_.size() > 0);
	assertrx_dbg(fieldBits <= 16);

	uint8_t control = 0;
	const bool useDelta = (vdocId_ >= previousVdocId);
	const uint32_t idToPack = useDelta ? (vdocId_ - previousVdocId) : vdocId_;
	if (useDelta) {
		control |= kIdDeltaBit;
	}
	const unsigned idBytes = byteLen32(idToPack);
	control |= uint8_t((idBytes - 1) << kIdLenShift);

	if (version_ <= kVersionMask) {
		control |= kVersionInlineBit;
		control |= uint8_t((version_ & kVersionMask) << kVersionShift);
	} else {
		const unsigned verBytes = byteLen32(version_);
		control |= uint8_t((verBytes - 1) << kVersionShift);
	}

	const bool simple = canPackSimple(pos_, fieldBits);
	if (!simple) {
		control |= kComplexBit;
	}

	*p++ = control;
	writeBytes(p, idToPack, idBytes);
	p += idBytes;
	if (!(control & kVersionInlineBit)) {
		const unsigned verBytes = ((control >> kVersionShift) & kVersionMask) + 1;
		writeBytes(p, version_, verBytes);
		p += verBytes;
	}

	if (simple) {
		if (storeArrayIdx) {
			assertrx_dbg(pos_[0].arrayIdx() <= 0xffu);
			*p++ = uint8_t(pos_[0].arrayIdx());
		} else {
			assertrx_dbg(pos_[0].arrayIdx() == 0);
		}
		const uint16_t packed = uint16_t((uint32_t(pos_[0].pos()) << fieldBits) | pos_[0].field());
		writeU16(p, packed);
		p += sizeof(packed);
		return size_t(p - buf);
	}

	const uint32_t count = uint32_t(pos_.size());
	p = packLen6(p, count);

	// After occurrence count: payload byte-length of the following hits.
	// count < kShortPayloadLenCount → 1 byte; otherwise 4 bytes (filled after hits are written).
	const unsigned payloadLenBytes = (count < kShortPayloadLenCount) ? 1u : 4u;
	uint8_t* const payloadLenPtr = p;
	p += payloadLenBytes;
	uint8_t* const hitsStart = p;

	for (const auto& pos : pos_) {
		if (storeArrayIdx) {
			p = packLen6(p, pos.arrayIdx());
		} else {
			assertrx_dbg(pos.arrayIdx() == 0);
		}

		const uint32_t posVal = pos.pos();
		const unsigned posBytes = byteLen32(posVal);
		assertrx_dbg(posBytes >= 1 && posBytes <= 4);
		assertrx_dbg(pos.field() <= kMaxFtCompositeFields);
		*p++ = uint8_t(((posBytes - 1) << 6) | (pos.field() & 0x3fu));
		writeBytes(p, posVal, posBytes);
		p += posBytes;
	}

	const uint32_t hitsLen = uint32_t(p - hitsStart);
	if (payloadLenBytes == 1) {
		assertrx_dbg(hitsLen <= 0xffu);
		*payloadLenPtr = uint8_t(hitsLen);
	} else {
		writeBytes(payloadLenPtr, hitsLen, 4);
	}

	return size_t(p - buf);
}

namespace {

const uint8_t* skipPosPayload(const uint8_t* p, [[maybe_unused]] const uint8_t* end, bool storeArrayIdx, bool simple) {
	if (simple) {
		if (storeArrayIdx) {
			assertrx_dbg(p < end);
			++p;
		}
		assertrx_dbg(size_t(end - p) >= sizeof(uint16_t));
		return p + sizeof(uint16_t);
	}

	uint32_t count = 0;
	p = unpackLen6(p, end, count);
	assertrx_dbg(count > 0);
	const unsigned payloadLenBytes = (count < kShortPayloadLenCount) ? 1u : 4u;
	assertrx_dbg(size_t(end - p) >= payloadLenBytes);
	const uint32_t hitsLen = uint32_t(readBytes(p, payloadLenBytes));
	p += payloadLenBytes;
	assertrx_dbg(size_t(end - p) >= hitsLen);
	return p + hitsLen;
}

}  // namespace

size_t IdRelTypePacked::unpackIdentity(const uint8_t* buf, uint32_t len, uint32_t previousVdocId, unsigned fieldBits, bool storeArrayIdx) {
	assertrx_dbg(len > 0);
	assertrx_dbg(fieldBits <= 16);
	const uint8_t* p = buf;
	const uint8_t* const end = buf + len;

	const uint8_t control = *p++;
	const unsigned idBytes = ((control >> kIdLenShift) & kIdLenMask) + 1;
	assertrx_dbg(size_t(end - p) >= idBytes);
	uint32_t idVal = uint32_t(readBytes(p, idBytes));
	p += idBytes;
	if (control & kIdDeltaBit) {
		idVal += previousVdocId;
	}
	vdocId_ = idVal;

	if (control & kVersionInlineBit) {
		version_ = (control >> kVersionShift) & kVersionMask;
	} else {
		const unsigned verBytes = ((control >> kVersionShift) & kVersionMask) + 1;
		assertrx_dbg(size_t(end - p) >= verBytes);
		version_ = readBytes(p, verBytes);
		p += verBytes;
	}

	packedSimple_ = !(control & kComplexBit);
	packedStoreArrayIdx_ = storeArrayIdx;
	packedFieldBits_ = uint8_t(fieldBits);
	packedPos_ = p;
	const uint8_t* const posEnd = skipPosPayload(p, end, storeArrayIdx, packedSimple_);
	packedPosLen_ = uint32_t(posEnd - p);
	positionsReady_ = false;
	pos_.clear();

	return size_t(posEnd - buf);
}

void IdRelTypePacked::ensurePositionsUnpacked() {
	if (positionsReady_) {
		return;
	}
	unpackPositionsFromPacked();
}

PosType IdRelTypePacked::PeekSimplePos() const {
	assertrx_dbg(IsSimple());
	if (positionsReady_) {
		return pos_[0];
	}
	assertrx_dbg(packedPosLen_ > 0);
	const uint8_t* p = packedPos_;
	[[maybe_unused]] const uint8_t* const end = packedPos_ + packedPosLen_;
	uint32_t arrayIdx = 0;
	if (packedStoreArrayIdx_) {
		assertrx_dbg(p < end);
		arrayIdx = *p++;
	}
	assertrx_dbg(size_t(end - p) >= sizeof(uint16_t));
	const uint16_t packed = readU16(p);
	const unsigned fieldBits = packedFieldBits_;
	const uint32_t field = fieldBits ? (packed & ((uint16_t(1) << fieldBits) - 1)) : 0;
	const uint32_t pos = packed >> fieldBits;
	return PosType(pos, field, arrayIdx);
}

void IdRelTypePacked::unpackPositionsFromPacked() {
	assertrx_dbg(packedPos_);
	assertrx_dbg(!positionsReady_);
	const uint8_t* p = packedPos_;
	const uint8_t* const end = packedPos_ + packedPosLen_;
	const unsigned fieldBits = packedFieldBits_;
	const bool storeArrayIdx = packedStoreArrayIdx_;

	if (packedSimple_) {
		uint32_t arrayIdx = 0;
		if (storeArrayIdx) {
			assertrx_dbg(p < end);
			arrayIdx = *p++;
		}
		assertrx_dbg(size_t(end - p) >= sizeof(uint16_t));
		const uint16_t packed = readU16(p);
		const uint32_t field = fieldBits ? (packed & ((uint16_t(1) << fieldBits) - 1)) : 0;
		const uint32_t pos = packed >> fieldBits;
		pos_.resize(1);
		pos_[0] = PosType(pos, field, arrayIdx);
		assertrx_dbg(p + sizeof(uint16_t) == end);
	} else {
		uint32_t count = 0;
		p = unpackLen6(p, end, count);
		assertrx_dbg(count > 0);
		const unsigned payloadLenBytes = (count < kShortPayloadLenCount) ? 1u : 4u;
		assertrx_dbg(size_t(end - p) >= payloadLenBytes);
		const uint32_t hitsLen = uint32_t(readBytes(p, payloadLenBytes));
		p += payloadLenBytes;
		assertrx_dbg(size_t(end - p) >= hitsLen);
		[[maybe_unused]] const uint8_t* const hitsEnd = p + hitsLen;

		pos_.resize(count);
		for (uint32_t i = 0; i < count; ++i) {
			uint32_t arrayIdx = 0;
			if (storeArrayIdx) {
				p = unpackLen6(p, end, arrayIdx);
			}
			assertrx_dbg(p < end);
			const uint8_t hdr = *p++;
			const unsigned posBytes = (hdr >> 6) + 1;
			const uint32_t field = hdr & 0x3fu;
			assertrx_dbg(size_t(end - p) >= posBytes);
			const uint32_t pos = uint32_t(readBytes(p, posBytes));
			p += posBytes;
			pos_[i] = PosType(pos, field, arrayIdx);
		}
		assertrx_dbg(p == hitsEnd);
		assertrx_dbg(p == end);
	}

	positionsReady_ = true;
	packedPos_ = nullptr;
	packedPosLen_ = 0;
}

}  // namespace reindexer
