#pragma once

#include <limits.h>
#include <algorithm>
#include "estl/h_vector.h"
#include "sort/pdqsort.hpp"
#include "tools/assertrx.h"

namespace reindexer {

typedef uint32_t VDocIdType;
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

// the position of the word in the document (the index of the word in the field (pos), the field in which the word field was
// encountered (field)
class [[nodiscard]] IdRelType {
public:
	explicit IdRelType(VDocIdType id = 0) noexcept : id_(id) {}
	IdRelType(IdRelType&&) noexcept = default;
	IdRelType(const IdRelType&) = default;
	IdRelType& operator=(IdRelType&&) noexcept = default;
	IdRelType& operator=(const IdRelType&) = default;

	VDocIdType Id() const noexcept { return id_; }

	// PackedIdRelVec callbacks
	size_t pack(uint8_t* buf, VDocIdType previousId, uint32_t previousField) const;
	size_t unpack(const uint8_t* buf, uint32_t len, VDocIdType previousId, uint32_t previousField);
	size_t packWithoutArrayIdxs(uint8_t* buf, VDocIdType previousId, uint32_t previousField) const;
	size_t unpackWithoutArrayIdxs(const uint8_t* buf, uint32_t len, VDocIdType previousId, uint32_t previousField);

	size_t maxpackedsize() const { return 2 * (sizeof(VDocIdType) + 1) + (pos_.size() * (sizeof(uint32_t) + 1)); }

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

	size_t size() const noexcept { return pos_.size(); }

	void SimpleCommit() noexcept {
		boost::sort::pdqsort_branchless(pos_.begin(), pos_.end(),
										[](const PosType& lhs, const PosType& rhs) noexcept { return lhs.pos() < rhs.pos(); });
	}

	const h_vector<PosType, 3>& Pos() const noexcept { return pos_; }
	h_vector<PosType, 3>& Pos() noexcept { return pos_; }

	size_t HeapSize() const noexcept { return pos_.heap_size(); }

	bool ArrayDataFound() {
		for (const auto& p : Pos()) {
			if (p.arrayIdx() > 0) {
				return true;
			}
		}

		return false;
	}

private:
	h_vector<PosType, 3> pos_;
	VDocIdType id_ = 0;	 // index of the document in which the word occurs
};

class [[nodiscard]] IdRelSet : public std::vector<IdRelType> {
public:
	int Add(VDocIdType id, unsigned pos, unsigned field, unsigned arrayIdx) {
		if (id > max_id_) {
			max_id_ = id;
		}
		if (id < min_id_) {
			min_id_ = id;
		}

		auto& last = (empty() || back().Id() != id) ? emplace_back(id) : back();
		last.Add(pos, field, arrayIdx);
		return last.size();
	}
	void SimpleCommit() noexcept {
		for (auto& val : *this) {
			val.SimpleCommit();
		}
	}

	VDocIdType max_id_ = 0;
	VDocIdType min_id_ = INT_MAX;
};

class [[nodiscard]] PackedIdRelVec {
public:
	typedef IdRelType value_type;
	typedef unsigned size_type;
	typedef IdRelType* pointer;
	typedef IdRelType& reference;
	typedef const IdRelType* const_pointer;
	typedef const IdRelType& const_reference;

	using store_container = std::vector<uint8_t>;

	struct [[nodiscard]] state {
		size_type size = 0;
		VDocIdType lastId = 0;
		unsigned lastField = 0;
	};

	class [[nodiscard]] iterator {
	public:
		iterator(const PackedIdRelVec* pv, store_container::const_iterator it, state st, size_t arrayFoundPos)
			: pv_(pv), it_(it), st_(st), arrayFoundPos_(arrayFoundPos) {
			unpack();
		}

		iterator& operator++() {
			unpack();
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
				size_t curPos = it_ - pv_->data_.begin();
				if (curPos < arrayFoundPos_) {
					curItemSize_ = curItem_.unpackWithoutArrayIdxs(&*it_, pv_->data_.end() - it_, st_.lastId, st_.lastField);
				} else {
					curItemSize_ = curItem_.unpack(&*it_, pv_->data_.end() - it_, st_.lastId, st_.lastField);
				}

				st_.lastId = curItem_.Id();
				assertrx_dbg(curItem_.Pos().size() > 0);
				st_.lastField = curItem_.Pos()[0].field();
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
		// no array data any more
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

			if (p < arrayFoundPos_) {
				if (it->ArrayDataFound()) {
					arrayFoundPos_ = p;
				}
			}

			if (p >= arrayFoundPos_) {
				p += it->pack(&*(data_.begin() + p), st_.lastId, st_.lastField);
			} else {
				p += it->packWithoutArrayIdxs(&*(data_.begin() + p), st_.lastId, st_.lastField);
			}

			st_.lastId = it->Id();
			assertrx_dbg(it->Pos().size() > 0);
			st_.lastField = it->Pos()[0].field();
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
	}
	bool empty() const noexcept { return st_.size == 0; }

	void get_state(state& st, size_t& dataSize) {
		st = st_;
		dataSize = data_.size();
	}

private:
	store_container data_;
	state st_;
	size_t arrayFoundPos_ = std::numeric_limits<size_t>::max();
};

class [[nodiscard]] IdRelVec : public std::vector<IdRelType> {
public:
	size_t heap_size() const noexcept {
		size_t res = capacity() * sizeof(IdRelType);
		for (const auto& id : *this) {
			res += id.HeapSize();
		}
		return res;
	}
	void erase_back(size_t pos) noexcept { erase(begin() + pos, end()); }
	size_t pos(const_iterator it) const noexcept { return it - cbegin(); }
};

}  // namespace reindexer
