#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string_view>
#include <vector>
#include "indextexttypes.h"
#include "tools/assertrx.h"

namespace reindexer {

static_assert(sizeof(WordIdType) == 4, "Word offsets must fit into 4 bytes");

struct [[nodiscard]] SuffixWordInfo {
	WordIdType wordId = 0;
	uint32_t offset = 0;  // chars before the suffix key inside the word
	uint32_t length = 0;  // full word length (without trailing 0)
};

class [[nodiscard]] WordsStorage {
public:
	static constexpr size_t kWordMetaPrefixLen = 2;
	static constexpr char16_t kWordMetaTag = 0x8000;

	WordIdType Add(std::u16string_view word, size_t wordOrdinal) {
		assertrx_throw(wordOrdinal < (size_t(1) << 30));
		const size_t offset = words_.size();
		assertrx_throw(offset + kWordMetaPrefixLen <= std::numeric_limits<WordIdType>::max());
		assertrx_throw(word.size() <= std::numeric_limits<WordIdType>::max() - offset - kWordMetaPrefixLen - 1);
		writeWordOrdinal(wordOrdinal);
		words_.insert(words_.end(), word.begin(), word.end());
		words_.emplace_back(0);
		return WordIdType(offset + kWordMetaPrefixLen);
	}

	std::u16string_view GetWord(WordIdType id) const noexcept {
		assertrx_dbg(id < words_.size());
		const auto begin = words_.begin() + id;
		const auto end = std::find(begin, words_.end(), char16_t{0});
		assertrx_dbg(end != words_.end());
		return std::u16string_view(words_.data() + id, size_t(end - begin));
	}

	size_t GetWordOrdinal(WordIdType id) const noexcept {
		assertrx_dbg(id >= kWordMetaPrefixLen);
		return readWordOrdinal(words_.data() + id - kWordMetaPrefixLen);
	}

	const char16_t* GetWordData(WordIdType id) const noexcept {
		assertrx_dbg(id < words_.size());
		return words_.data() + id;
	}

	// Resolve suffix key to wordId + offset + length.
	// Layout: [0][meta][word][0][meta][word][0]... — leading 0 is a sentinel before the first word.
	SuffixWordInfo ResolveSuffixId(WordIdType suffixId) const noexcept {
		assertrx_dbg(suffixId < words_.size());
		assertrx_dbg(words_[suffixId] != 0);

		size_t end = suffixId;
		while (end < words_.size() && words_[end] != 0) {
			++end;
		}

		size_t p = suffixId;
		do {
			assertrx_dbg(p > 0);
			--p;
		} while (words_[p] != 0);

		const auto wordId = WordIdType(p + 1 + kWordMetaPrefixLen);
		assertrx_dbg(suffixId >= wordId);
		assertrx_dbg(end >= wordId);
		return {wordId, uint32_t(suffixId - wordId), uint32_t(end - wordId)};
	}

	void Clear() noexcept {
		words_.clear();
		words_.assign(1, char16_t{0});
	}
	size_t size() const noexcept { return words_.size(); }
	size_t heap_size() const noexcept { return words_.capacity() * sizeof(typename decltype(words_)::value_type); }

private:
	void writeWordOrdinal(size_t wordOrdinal) {
		const uint32_t v = uint32_t(wordOrdinal) + 1;
		words_.emplace_back(char16_t((v & 0x7FFF) | kWordMetaTag));
		words_.emplace_back(char16_t(((v >> 15) & 0x7FFF) | kWordMetaTag));
	}

	static size_t readWordOrdinal(const char16_t* meta) noexcept {
		const uint32_t v = (meta[0] & 0x7FFF) | (uint32_t(meta[1] & 0x7FFF) << 15);
		return size_t(v - 1);
	}

	// Sentinel 0 before the first word — same delimiter as between words.
	std::vector<char16_t> words_{char16_t{0}};
};

}  // namespace reindexer
