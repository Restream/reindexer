#pragma once

#include <cstdint>
#include <string_view>
#include "core/ft/limits.h"
#include "core/ft/typos.h"
#include "estl/h_vector.h"
#include "indextexttypes.h"
#include "tools/assertrx.h"
#include "vendor/hopscotch/hopscotch_set.h"
#include "wordsstorage.h"

namespace reindexer {

// Typo key layout (6 bytes, little-endian):
//   [0..3] wordId
//   [4]    first skipped position (kTypoMissingPosition = nothing skipped)
//   [5]    second skipped position
using TypoKey = uint64_t;
static constexpr uint8_t kTypoMissingPosition = 0xFF;
static constexpr uint32_t kTypoKeyBytes = 6;
static constexpr TypoKey kTypoWordIdMask = TypoKey{0xFFFFFFFF};
static constexpr TypoKey kTypoByteMask = 0xFF;
static constexpr uint32_t kMaxTypoWordLen = kTypoMissingPosition;
static_assert(kMaxTypoLenLimit < kTypoMissingPosition, "Typo position must be less than kTypoMissingPosition");

using TypoKeyGroup = h_vector<uint8_t, 12>;

static inline TypoKey PackTypoKey(WordIdType wordId, const TyposVec& positions) noexcept {
	assertrx_dbg(positions.size() <= kMaxTyposInWord);
	assertrx_dbg(positions.size() == 0 || positions[0] < kTypoMissingPosition);
	assertrx_dbg(positions.size() <= 1 || positions[1] < kTypoMissingPosition);
	TypoKey key = wordId;
	key |= TypoKey(positions.size() > 0 ? positions[0] : kTypoMissingPosition) << 32;
	key |= TypoKey(positions.size() > 1 ? positions[1] : kTypoMissingPosition) << 40;
	return key;
}

static inline WordIdType UnpackTypoWordId(TypoKey key) noexcept { return WordIdType(key & kTypoWordIdMask); }

static inline uint8_t UnpackTypoPosition0(TypoKey key) noexcept { return uint8_t((key >> 32) & kTypoByteMask); }

static inline uint8_t UnpackTypoPosition1(TypoKey key) noexcept { return uint8_t((key >> 40) & kTypoByteMask); }

static inline void AppendTypoKey(TypoKeyGroup& group, TypoKey key) {
	for (uint32_t i = 0; i < kTypoKeyBytes; ++i) {
		group.emplace_back(uint8_t(key >> (8 * i)));
	}
}

static inline size_t TypoKeysCount(const TypoKeyGroup& group) noexcept {
	assertrx_dbg(group.size() % kTypoKeyBytes == 0);
	return group.size() / kTypoKeyBytes;
}

static inline TypoKey GetTypoKey(const TypoKeyGroup& group, size_t idx) noexcept {
	assertrx_dbg(idx < TypoKeysCount(group));
	const size_t offset = idx * kTypoKeyBytes;
	TypoKey key = 0;
	for (uint32_t i = 0; i < kTypoKeyBytes; ++i) {
		key |= TypoKey(group[offset + i]) << (8 * i);
	}
	return key;
}

static inline TypoKey GetFirstTypoKey(const TypoKeyGroup& group) noexcept {
	assertrx_dbg(!group.empty());
	return GetTypoKey(group, 0);
}

static inline bool IsTypoPositionSkipped(TypoKey key, size_t pos) noexcept {
	const auto pos0 = UnpackTypoPosition0(key);
	const auto pos1 = UnpackTypoPosition1(key);
	return (pos0 != kTypoMissingPosition && pos == pos0) || (pos1 != kTypoMissingPosition && pos == pos1);
}

static inline size_t HashTypoWord(const char16_t* word, uint8_t pos0, uint8_t pos1) noexcept {
	size_t hash = 1469598103934665603ULL;
	const char16_t* p = word;
	if (pos0 != kTypoMissingPosition) {
		while (p < word + pos0) {
			hash = hash * 1099511628211ULL + *p++;
		}
		++p;
		if (pos1 != kTypoMissingPosition) {
			while (p < word + pos1) {
				hash = hash * 1099511628211ULL + *p++;
			}
			++p;
		}
	}
	while (char16_t ch = *p++) {
		hash = hash * 1099511628211ULL + ch;
	}
	return hash;
}

static inline bool EqualTypoWord(const char16_t* word, uint8_t pos0, uint8_t pos1, std::u16string_view rhs) noexcept {
	const char16_t* p = word;
	size_t rhsPos = 0;
	if (pos0 != kTypoMissingPosition) {
		while (p < word + pos0) {
			if (rhsPos >= rhs.size() || *p != rhs[rhsPos]) {
				return false;
			}
			++p;
			++rhsPos;
		}
		++p;
		if (pos1 != kTypoMissingPosition) {
			while (p < word + pos1) {
				if (rhsPos >= rhs.size() || *p != rhs[rhsPos]) {
					return false;
				}
				++p;
				++rhsPos;
			}
			++p;
		}
	}
	while (char16_t ch = *p++) {
		if (rhsPos >= rhs.size() || ch != rhs[rhsPos]) {
			return false;
		}
		++rhsPos;
	}
	return rhsPos == rhs.size();
}

class [[nodiscard]] TypoKeyHash {
public:
	using is_transparent = void;

	explicit TypoKeyHash(const WordsStorage& words) noexcept : words_(&words) {}

	size_t operator()(const TypoKeyGroup& group) const noexcept {
		assertrx_dbg(!group.empty());
		// Only the first packed TypoKey participates in the set key identity.
		return hashTypo(GetFirstTypoKey(group));
	}
	size_t operator()(std::u16string_view typo) const noexcept { return hashWord(typo); }

private:
	size_t hashTypo(TypoKey key) const noexcept {
		return HashTypoWord(words_->GetWordData(UnpackTypoWordId(key)), UnpackTypoPosition0(key), UnpackTypoPosition1(key));
	}
	static size_t hashWord(std::u16string_view word) noexcept {
		size_t hash = 1469598103934665603ULL;
		for (char16_t ch : word) {
			hash = hash * 1099511628211ULL + ch;
		}
		return hash;
	}

	const WordsStorage* words_;
};

class [[nodiscard]] TypoKeyEqual {
public:
	using is_transparent = void;

	explicit TypoKeyEqual(const WordsStorage& words) noexcept : words_(&words) {}

	bool operator()(const TypoKeyGroup& lhs, const TypoKeyGroup& rhs) const noexcept {
		assertrx_dbg(!lhs.empty());
		assertrx_dbg(!rhs.empty());
		// Only the first packed TypoKey participates in the set key identity.
		return equalTypo(GetFirstTypoKey(lhs), GetFirstTypoKey(rhs));
	}
	bool operator()(const TypoKeyGroup& lhs, std::u16string_view rhs) const noexcept {
		assertrx_dbg(!lhs.empty());
		return equalTypo(GetFirstTypoKey(lhs), rhs);
	}
	bool operator()(std::u16string_view lhs, const TypoKeyGroup& rhs) const noexcept {
		assertrx_dbg(!rhs.empty());
		return equalTypo(GetFirstTypoKey(rhs), lhs);
	}

private:
	bool equalTypo(TypoKey lhs, TypoKey rhs) const noexcept {
		if (lhs == rhs) {
			return true;
		}
		const auto lhsWord = words_->GetWordData(UnpackTypoWordId(lhs));
		const auto rhsWord = words_->GetWordData(UnpackTypoWordId(rhs));
		size_t lhsPos = 0, rhsPos = 0;
		while (true) {
			while (lhsWord[lhsPos] && IsTypoPositionSkipped(lhs, lhsPos)) {
				++lhsPos;
			}
			while (rhsWord[rhsPos] && IsTypoPositionSkipped(rhs, rhsPos)) {
				++rhsPos;
			}
			if (!lhsWord[lhsPos] || !rhsWord[rhsPos]) {
				return !lhsWord[lhsPos] && !rhsWord[rhsPos];
			}
			if (lhsWord[lhsPos++] != rhsWord[rhsPos++]) {
				return false;
			}
		}
	}
	bool equalTypo(TypoKey lhs, std::u16string_view rhs) const noexcept {
		return EqualTypoWord(words_->GetWordData(UnpackTypoWordId(lhs)), UnpackTypoPosition0(lhs), UnpackTypoPosition1(lhs), rhs);
	}

	const WordsStorage* words_;
};

// TypoSet stores one TypoKeyGroup per distinct typo string.
//
// Contract (do not break casually — hopscotch_set relies on it):
// 1. Hash and equality of a group are defined ONLY by its first TypoKey
//    (see TypoKeyHash / TypoKeyEqual → GetFirstTypoKey). Extra keys in the
//    group are payload: other wordId+skip packs that produce the same typo.
// 2. After insert we may APPEND more TypoKeys to an existing group
//    (AddTypoKey → AppendTypoKey on *it). That is intentional: it avoids a
//    separate value container while keeping hash/equal stable.
// 3. Allowed mutations of a live set element: append-only. Forbidden without
//    erase+reinsert: changing/removing the first key, reordering so another
//    key becomes first, or any edit that would change GetFirstTypoKey's
//    virtual typo string (and thus the element's hash/bucket).
//
// Rationale: several indexed words can collapse to the same typo string; we
// group their TypoKeys under one set node keyed by that string, without
// materializing typo text in the container.
using TypoSet = tsl::hopscotch_set<TypoKeyGroup, TypoKeyHash, TypoKeyEqual>;

}  // namespace reindexer
