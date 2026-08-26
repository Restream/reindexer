#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>
#include "cpp-btree/btree_set.h"
#include "indextexttypes.h"
#include "tools/assertrx.h"
#include "wordsstorage.h"

namespace reindexer {

// Suffix key is an index into flat word storage: wordId + offset.
using SuffixKey = uint32_t;

static inline SuffixKey PackSuffixKey(WordIdType wordId, uint32_t textOffset) noexcept {
	const auto suffixId = size_t(wordId) + textOffset;
	assertrx_dbg(suffixId <= std::numeric_limits<SuffixKey>::max());
	return SuffixKey(suffixId);
}

static inline int SuffixCompare(const char16_t* lhs, const char16_t* rhs) noexcept {
	while (char16_t lch = *lhs++) {
		const char16_t rch = *rhs++;
		if (lch != rch) {
			return lch < rch ? -1 : 1;
		}
	}
	return *rhs != 0 ? -1 : 0;
}

static inline int SuffixCompare(const char16_t* lhs, std::u16string_view rhs) noexcept {
	size_t pos = 0;
	while (char16_t lch = *lhs++) {
		if (pos >= rhs.size()) {
			return 1;
		}
		const char16_t rch = rhs[pos++];
		if (lch != rch) {
			return lch < rch ? -1 : 1;
		}
	}
	return pos < rhs.size() ? -1 : 0;
}

static inline int SuffixCompare(std::u16string_view lhs, const char16_t* rhs) noexcept {
	size_t pos = 0;
	while (pos < lhs.size()) {
		const char16_t lch = lhs[pos++];
		const char16_t rch = *rhs++;
		if (lch != rch) {
			return lch < rch ? -1 : 1;
		}
	}
	return *rhs != 0 ? -1 : 0;
}

static inline bool SuffixStartsWith(const char16_t* suffix, std::u16string_view prefix) noexcept {
	size_t pos = 0;
	while (pos < prefix.size()) {
		if (suffix[pos] != prefix[pos]) {
			return false;
		}
		++pos;
	}
	return true;
}

class [[nodiscard]] SuffixKeyCompare {
public:
	explicit SuffixKeyCompare(const WordsStorage& words) noexcept : words_(&words) {}

	bool operator()(SuffixKey lhs, SuffixKey rhs) const noexcept {
		const auto* ldata = words_->GetWordData(lhs);
		const auto* rdata = words_->GetWordData(rhs);
		const int cmp = SuffixCompare(ldata, rdata);
		return cmp != 0 ? cmp < 0 : lhs < rhs;
	}
	bool operator()(SuffixKey lhs, std::u16string_view rhs) const noexcept { return SuffixCompare(words_->GetWordData(lhs), rhs) < 0; }
	bool operator()(std::u16string_view lhs, SuffixKey rhs) const noexcept { return SuffixCompare(lhs, words_->GetWordData(rhs)) < 0; }

private:
	const WordsStorage* words_;
};

using SuffixTree = btree::btree_set<SuffixKey, SuffixKeyCompare, std::allocator<SuffixKey>, 256>;

}  // namespace reindexer
