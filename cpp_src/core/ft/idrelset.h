#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <tuple>
#include "estl/h_vector.h"
#include "sort/pdqsort.hpp"
#include "tools/assertrx.h"

namespace reindexer {

using VDocVersion = uint32_t;
inline constexpr VDocVersion kEmptyVDocVersion = 0;
inline constexpr VDocVersion kMaxVDocVersion = std::numeric_limits<VDocVersion>::max();
inline constexpr uint32_t kEmptyVDocId = 0;	 // sentinel vdocs_[0]; stale postings also resolve to this

struct [[nodiscard]] VDocPosting {
	uint32_t vdocId = kEmptyVDocId;
	VDocVersion version = kEmptyVDocVersion;
};

static constexpr int kMaxFtCompositeFields = 63;

class [[nodiscard]] PosType {
public:
	PosType() = default;
	PosType(uint64_t pos, uint64_t field, uint64_t arrayIdx) noexcept
		: fpos_(pos | (arrayIdx << posBits) | (field << (posBits + arrayIdxBits))) {}
	uint32_t field() const noexcept { return fpos_ >> (posBits + arrayIdxBits); }
	uint32_t fullField() const noexcept { return fpos_ >> posBits; }
	uint32_t arrayIdx() const noexcept { return (fpos_ >> posBits) & ((1 << arrayIdxBits) - 1); }
	uint32_t pos() const noexcept { return fpos_ & ((1 << posBits) - 1); }
	uint32_t fullPos() const noexcept { return fpos_; }
	bool operator<(PosType other) const noexcept { return fpos_ < other.fpos_; }
	bool operator==(PosType other) const noexcept { return fpos_ == other.fpos_; }

private:
	static const int posBits = 28;
	static const int arrayIdxBits = 28;

	uint64_t fpos_ = 0;
};

using PositionsVector = h_vector<PosType, 2>;

class [[nodiscard]] PosTypeSimple {
public:
	PosTypeSimple() = default;
	PosTypeSimple(uint32_t pos, uint32_t field, uint32_t /*arrayIdx*/) noexcept : fpos_(pos | (field << posBits)) {}
	uint32_t pos() const noexcept { return fpos_ & ((1 << posBits) - 1); }
	uint32_t field() const noexcept { return fpos_ >> posBits; }
	uint32_t arrayIdx() const noexcept { return 0; }
	uint32_t fullField() const noexcept { return field(); }
	uint32_t fullPos() const noexcept { return fpos_; }
	bool operator<(PosTypeSimple other) const noexcept { return fpos_ < other.fpos_; }
	bool operator==(PosTypeSimple other) const noexcept { return fpos_ == other.fpos_; }

private:
	static const int posBits = 24;
	uint32_t fpos_;
};

struct [[nodiscard]] PosTypeDebug : public PosType {
	PosTypeDebug() = default;
	explicit PosTypeDebug(const PosType& pos, const std::string& inf) : PosType(pos), info(inf) {}
	explicit PosTypeDebug(const PosType& pos, std::string&& inf) noexcept : PosType(pos), info(std::move(inf)) {}
	std::string info;
};

// CPU / build-time posting: always materialized positions (no lazy packed state).
class [[nodiscard]] IdRelType {
public:
	explicit IdRelType(uint32_t vdocId = kEmptyVDocId, VDocVersion version = kEmptyVDocVersion) noexcept
		: vdocId_(vdocId), version_(version) {}

	uint32_t VdocId() const noexcept { return vdocId_; }
	VDocVersion VdocVersion() const noexcept { return version_; }

	// Encode into PackedIdRelVec blob (from materialized Pos()).
	// storeArrayIdx: when false, arrayIdx is implied 0 and not stored.
	size_t pack(uint8_t* buf, uint32_t previousVdocId, unsigned fieldBits, bool storeArrayIdx) const;

	size_t maxpackedsize() const noexcept {
		// control + id + version + optional arrayIdx + count + payloadLen(+4) + hits
		return 1 + 4 + 4 + 1 + 5 + 4 + pos_.size() * (1 + 4 + 1 + 4);
	}

	void reserve(int s) { pos_.reserve(s); }
	bool empty() const noexcept { return pos_.empty(); }

	void Add(unsigned pos, unsigned field, unsigned arrayIdx) {
		assertrx_throw(field <= kMaxFtCompositeFields);
		pos_.emplace_back(pos, field, arrayIdx);
	}

	void Add(PosType p) { pos_.emplace_back(p); }

	void SortAndUnique() {
		boost::sort::pdqsort_branchless(pos_.begin(), pos_.end());
		auto last = std::unique(pos_.begin(), pos_.end());
		pos_.resize(last - pos_.begin());
	}

	void Clear() noexcept {
#ifdef REINDEXER_FT_EXTRA_DEBUG
		pos_.clear<false>();
#else
		pos_.clear();
#endif
	}

	bool IsSimple() const noexcept { return pos_.size() == 1; }

	PosType PeekSimplePos() const noexcept {
		assertrx_dbg(IsSimple());
		return pos_[0];
	}

	size_t size() const noexcept { return pos_.size(); }

	void SimpleCommit() noexcept {
		boost::sort::pdqsort_branchless(pos_.begin(), pos_.end(),
										[](const PosType& lhs, const PosType& rhs) noexcept { return lhs.pos() < rhs.pos(); });
	}

	const PositionsVector& Pos() const noexcept { return pos_; }
	PositionsVector& Pos() noexcept { return pos_; }

	size_t HeapSize() const noexcept { return pos_.heap_size(); }

	bool ArrayDataFound() const noexcept {
		for (const auto& p : pos_) {
			if (p.arrayIdx() > 0) {
				return true;
			}
		}
		return false;
	}

private:
	PositionsVector pos_;
	uint32_t vdocId_ = kEmptyVDocId;
	VDocVersion version_ = kEmptyVDocVersion;
};

// Memory-optimized posting view used by PackedIdRelVec::iterator (lazy position unpack).
class [[nodiscard]] IdRelTypePacked {
public:
	explicit IdRelTypePacked(uint32_t vdocId = kEmptyVDocId, VDocVersion version = kEmptyVDocVersion) noexcept
		: vdocId_(vdocId), version_(version) {}
	IdRelTypePacked(const IdRelTypePacked& other) = default;
	IdRelTypePacked(IdRelTypePacked&& other) noexcept
		: pos_(std::move(other.pos_)),
		  vdocId_(other.vdocId_),
		  version_(other.version_),
		  packedPos_(other.packedPos_),
		  packedPosLen_(other.packedPosLen_),
		  packedFieldBits_(other.packedFieldBits_),
		  packedStoreArrayIdx_(other.packedStoreArrayIdx_),
		  packedSimple_(other.packedSimple_),
		  positionsReady_(other.positionsReady_) {
		other.vdocId_ = kEmptyVDocId;
		other.version_ = kEmptyVDocVersion;
		other.packedPos_ = nullptr;
		other.packedPosLen_ = 0;
		other.packedFieldBits_ = 0;
		other.packedStoreArrayIdx_ = false;
		other.packedSimple_ = false;
		other.positionsReady_ = true;
	}

	IdRelTypePacked& operator=(const IdRelTypePacked& other) = default;
	IdRelTypePacked& operator=(IdRelTypePacked&& other) noexcept {
		if (this != &other) {
			pos_ = std::move(other.pos_);
			vdocId_ = other.vdocId_;
			version_ = other.version_;
			packedPos_ = other.packedPos_;
			packedPosLen_ = other.packedPosLen_;
			packedFieldBits_ = other.packedFieldBits_;
			packedStoreArrayIdx_ = other.packedStoreArrayIdx_;
			packedSimple_ = other.packedSimple_;
			positionsReady_ = other.positionsReady_;
			other.vdocId_ = kEmptyVDocId;
			other.version_ = kEmptyVDocVersion;
			other.packedPos_ = nullptr;
			other.packedPosLen_ = 0;
			other.packedFieldBits_ = 0;
			other.packedStoreArrayIdx_ = false;
			other.packedSimple_ = false;
			other.positionsReady_ = true;
		}
		return *this;
	}

	uint32_t VdocId() const noexcept { return vdocId_; }
	VDocVersion VdocVersion() const noexcept { return version_; }

	// Decodes vdocId/version and measures full record size without materializing positions.
	size_t unpackIdentity(const uint8_t* buf, uint32_t len, uint32_t previousVdocId, unsigned fieldBits, bool storeArrayIdx);

	void reserve(int s) {
		ensurePositionsUnpacked();
		pos_.reserve(s);
	}
	bool empty() {
		if (!positionsReady_) {
			return packedPosLen_ == 0;
		}
		return pos_.empty();
	}

	void Add(unsigned pos, unsigned field, unsigned arrayIdx) {
		assertrx_throw(field <= kMaxFtCompositeFields);
		ensurePositionsUnpacked();
		pos_.emplace_back(pos, field, arrayIdx);
	}

	void Add(PosType p) {
		ensurePositionsUnpacked();
		pos_.emplace_back(p);
	}

	void SortAndUnique() {
		ensurePositionsUnpacked();
		boost::sort::pdqsort_branchless(pos_.begin(), pos_.end());
		auto last = std::unique(pos_.begin(), pos_.end());
		pos_.resize(last - pos_.begin());
	}

	void Clear() noexcept {
		packedPos_ = nullptr;
		packedPosLen_ = 0;
		positionsReady_ = true;
#ifdef REINDEXER_FT_EXTRA_DEBUG
		pos_.clear<false>();
#else
		pos_.clear();
#endif
	}

	bool IsSimple() const noexcept {
		if (!positionsReady_) {
			return packedSimple_ && packedPos_;
		}
		return pos_.size() == 1;
	}

	// Decode the single simple hit without materializing PositionsVector when still packed.
	PosType PeekSimplePos() const;

	size_t size() {
		if (!positionsReady_ && packedSimple_) {
			return 1;
		}
		ensurePositionsUnpacked();
		return pos_.size();
	}

	void SimpleCommit() {
		ensurePositionsUnpacked();
		boost::sort::pdqsort_branchless(pos_.begin(), pos_.end(),
										[](const PosType& lhs, const PosType& rhs) noexcept { return lhs.pos() < rhs.pos(); });
	}

	PositionsVector& Pos() {
		ensurePositionsUnpacked();
		return pos_;
	}

	PositionsVector TakePos() {
		ensurePositionsUnpacked();
		PositionsVector res = std::move(pos_);
		positionsReady_ = false;
		packedPos_ = nullptr;
		packedPosLen_ = 0;
		packedSimple_ = false;
		return res;
	}

	size_t HeapSize() {
		if (!positionsReady_) {
			// Packed view points into PackedIdRelVec blob; heap cost is counted on the vector.
			return 0;
		}
		return pos_.heap_size();
	}

	bool ArrayDataFound() {
		for (const auto& p : Pos()) {
			if (p.arrayIdx() > 0) {
				return true;
			}
		}
		return false;
	}

private:
	void ensurePositionsUnpacked();
	void unpackPositionsFromPacked();

	PositionsVector pos_;
	uint32_t vdocId_ = kEmptyVDocId;
	VDocVersion version_ = kEmptyVDocVersion;

	const uint8_t* packedPos_ = nullptr;
	uint32_t packedPosLen_ = 0;
	uint8_t packedFieldBits_ = 0;
	bool packedStoreArrayIdx_ = false;
	bool packedSimple_ = false;
	bool positionsReady_ = true;
};

class [[nodiscard]] IdRelSet : public std::vector<IdRelType> {
public:
	void Add(uint32_t vdocId, VDocVersion version, unsigned pos, unsigned field, unsigned arrayIdx) {
		auto& last = (empty() || back().VdocId() != vdocId || back().VdocVersion() != version) ? emplace_back(vdocId, version) : back();
		last.Add(pos, field, arrayIdx);
	}
	void SimpleCommit() {
		for (auto& val : *this) {
			val.SimpleCommit();
		}
	}
};

class [[nodiscard]] PackedIdRelVec {
public:
	typedef IdRelTypePacked value_type;
	typedef unsigned size_type;
	typedef IdRelTypePacked* pointer;
	typedef IdRelTypePacked& reference;
	typedef const IdRelTypePacked* const_pointer;
	typedef const IdRelTypePacked& const_reference;

	using store_container = std::vector<uint8_t>;

	static unsigned NumBitsForFields(size_t numFields) noexcept {
		if (numFields <= 1) {
			return 0;
		}
		unsigned bits = 0;
		for (size_t v = numFields - 1; v; v >>= 1) {
			++bits;
		}
		return bits;
	}

	PackedIdRelVec() = default;
	explicit PackedIdRelVec(unsigned fieldBits) noexcept : fieldBits_(fieldBits) {}

	unsigned FieldBits() const noexcept { return fieldBits_; }
	void SetFieldBits(unsigned fieldBits) noexcept { fieldBits_ = fieldBits; }

	struct [[nodiscard]] state {
		size_type size = 0;
		uint32_t lastVdocId = kEmptyVDocId;
		VDocVersion lastVersion = kEmptyVDocVersion;
	};

	class [[nodiscard]] iterator {
	public:
		iterator(const PackedIdRelVec* pv, store_container::const_iterator it, state st, size_t arrayFoundPos)
			: pv_(pv), it_(it), st_(st), arrayFoundPos_(arrayFoundPos) {
			std::ignore = unpack();
		}

		iterator& operator++() {
			std::ignore = unpack();
			it_ += curItemSize_;
			curItemSize_ = 0;
			return *this;
		}
		pointer operator->() { return &unpack(); }
		reference operator*() { return unpack(); }
		bool operator!=(const iterator& rhs) const noexcept { return it_ != rhs.it_; }
		bool operator==(const iterator& rhs) const noexcept { return it_ == rhs.it_; }

	private:
		reference unpack() {
			if (!curItemSize_ && it_ != pv_->data_.end()) {
				const size_t curPos = size_t(it_ - pv_->data_.begin());
				const bool storeArrayIdx = (curPos >= arrayFoundPos_);
				curItemSize_ =
					curItem_.unpackIdentity(&*it_, uint32_t(pv_->data_.end() - it_), st_.lastVdocId, pv_->fieldBits_, storeArrayIdx);
				st_.lastVdocId = curItem_.VdocId();
				st_.lastVersion = curItem_.VdocVersion();
			}
			return curItem_;
		}
		value_type curItem_;
		const PackedIdRelVec* pv_;
		store_container::const_iterator it_;
		size_type curItemSize_ = 0;
		state st_;
		size_t arrayFoundPos_ = std::numeric_limits<size_t>::max();
	};

	using const_iterator = const iterator;
	iterator begin() const { return iterator(this, data_.begin(), state(), arrayFoundPos_); }
	iterator end() const { return iterator(this, data_.end(), state(), arrayFoundPos_); }

	void erase_back(state st, size_t dataSize) {
		data_.resize(dataSize);
		st_ = st;
		if (arrayFoundPos_ > dataSize) {
			arrayFoundPos_ = std::numeric_limits<size_t>::max();
		}
	}

	size_type size() const noexcept { return st_.size; }

	template <typename InputIterator>
	void insert_back(InputIterator from, InputIterator to) {
		data_.reserve((to - from) / 2);
		int i = 0;
		size_type p = data_.size();
		for (auto it = from; it != to; ++it, ++i) {
			if (!(i % 128)) {
				size_type sz = 0, j = 0;
				for (auto iit = it; j < 128 && iit != to; iit++, j++) {
					sz += iit->maxpackedsize();
				}
				data_.resize(p + sz);
			}

			if (p < arrayFoundPos_ && it->ArrayDataFound()) {
				arrayFoundPos_ = p;
			}
			const bool storeArrayIdx = (p >= arrayFoundPos_);
			p += it->pack(&*(data_.begin() + p), st_.lastVdocId, fieldBits_, storeArrayIdx);

			st_.lastVdocId = it->VdocId();
			st_.lastVersion = it->VdocVersion();
			assertrx_dbg(it->Pos().size() > 0);
			assertrx(p <= data_.size());
		}
		data_.resize(p);
		st_.size += (to - from);
	}

	void shrink_to_fit() { data_.shrink_to_fit(); }
	size_type heap_size() noexcept { return data_.capacity(); }
	void clear() noexcept {
		data_.clear();
		st_ = state();
		arrayFoundPos_ = std::numeric_limits<size_t>::max();
	}
	bool empty() const noexcept { return st_.size == 0; }

	void get_state(state& st, size_t& dataSize) {
		st = st_;
		dataSize = data_.size();
	}

	const uint8_t* data() const noexcept { return data_.data(); }
	size_t data_size() const noexcept { return data_.size(); }
	size_t array_found_pos() const noexcept { return arrayFoundPos_; }

	// Adopt packed bytes [0, byteEnd) and packing state after that unchanged prefix.
	void InitFromUnchangedPrefix(const PackedIdRelVec& src, size_t byteEnd, state stAfterPrefix) {
		assertrx_dbg(byteEnd <= src.data_.size());
		assertrx_dbg(stAfterPrefix.size <= src.st_.size);
		data_.assign(src.data_.begin(), src.data_.begin() + ptrdiff_t(byteEnd));
		st_ = stAfterPrefix;
		arrayFoundPos_ = (src.arrayFoundPos_ < byteEnd) ? src.arrayFoundPos_ : std::numeric_limits<size_t>::max();
	}

private:
	store_container data_;
	state st_;
	unsigned fieldBits_ = 0;
	size_t arrayFoundPos_ = std::numeric_limits<size_t>::max();
};

class [[nodiscard]] IdRelVec : public std::vector<IdRelType> {
public:
	size_t heap_size() const noexcept {
		size_t res = capacity() * sizeof(IdRelType);
		for (const auto& vdocOccurence : *this) {
			res += vdocOccurence.HeapSize();
		}
		return res;
	}
	void erase_back(size_t pos) noexcept { erase(begin() + pos, end()); }
	size_t pos(const_iterator it) const noexcept { return it - cbegin(); }
};

}  // namespace reindexer
