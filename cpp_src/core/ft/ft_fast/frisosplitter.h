// this code is based on 'friso' https://github.com/lionsoul2014/friso
// Copyright(c) 2010 lionsoulchenxin619315 @gmail.com

// Permission is hereby granted,
// free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute,
// sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions :

// The above copyright notice and this permission notice shall be included in all copies
// or
// substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>
#include "estl/fast_hash_set.h"
#include "estl/h_vector.h"
#include "splitter.h"
#include "tools/stringstools.h"

namespace reindexer {
enum [[nodiscard]] FrisoLexType {
	__LEX_CJK_WORDS__ = 0,
	__LEX_CJK_UNITS__ = 1,
	__LEX_ECM_WORDS__ = 2,	// english and chinese mixed words.
	__LEX_CEM_WORDS__ = 3,	// chinese and english mixed words.
	__LEX_CN_LNAME__ = 4,
	__LEX_CN_SNAME__ = 5,
	__LEX_CN_LNA__ = 8,
	__LEX_STOPWORDS__ = 9,
	__LEX_ENPUN_WORDS__ = 10,
	__LEX_EN_WORDS__ = 11,
	__LEX_OTHER_WORDS__ = 15,
	__LEX_PUNC_WORDS__ = 17,	// punctuations
	__LEX_UNKNOW_WORDS__ = 18,	// unrecognized words.
	__LEX_MAX = 19
};

struct [[nodiscard]] WordInf {
	FrisoLexType type = __LEX_UNKNOW_WORDS__;
	std::string word;
	unsigned int wordChars = 0;
	std::vector<std::string> syn;
	unsigned int fre = 0;
};

struct [[nodiscard]] WordPos {
	size_t length = 0;
	size_t offset = 0;
};

class [[nodiscard]] FrisoTokenEntry {
public:
	FrisoTokenEntry() = default;
	FrisoTokenEntry(FrisoLexType t, WordPos _inStorage, WordPos _inOrigin, WordPos _inSymbols) noexcept
		: type_(t), inStorage_(_inStorage), inOrigin_(_inOrigin), inSymbols_(_inSymbols) {}
	FrisoLexType GetType() const noexcept { return type_; }
	WordPos GetInStorage() const noexcept { return inStorage_; }
	WordPos GetInOrigin() const noexcept { return inOrigin_; }
	WordPos GetInSymbols() const noexcept { return inSymbols_; }

private:
	FrisoLexType type_ = __LEX_UNKNOW_WORDS__;	// type of the word. (item of friso_lex_t)
	WordPos inStorage_;
	WordPos inOrigin_;
	WordPos inSymbols_;
};

class [[nodiscard]] StringBuffer {
public:
	void Reserve(size_t s) { modifiedTextWords_.reserve(s); }
	void NewWord() noexcept { modifiedTextWordsIndex_ = modifiedTextWords_.size(); }
	void AddString(std::string_view b) { modifiedTextWords_ += b; }
	size_t LastWordSize() const noexcept { return modifiedTextWords_.size() - modifiedTextWordsIndex_; }
	size_t LastWordOffset() const noexcept { return modifiedTextWordsIndex_; }
	std::string_view LastWord() const noexcept {
		return std::string_view(&modifiedTextWords_[0] + modifiedTextWordsIndex_, modifiedTextWords_.size() - modifiedTextWordsIndex_);
	}
	std::string_view GetString(size_t offset, size_t lenth) const noexcept {
		return std::string_view(&modifiedTextWords_[0] + offset, lenth);
	}
	void Resize(size_t v) { modifiedTextWords_.resize(v); }

private:
	std::string modifiedTextWords_;
	size_t modifiedTextWordsIndex_ = 0;
};

struct [[nodiscard]] FrisoConfig {
	static constexpr unsigned int kKeepPuncLen = 13;
	uint16_t max_len = 5;			  // the max match length (4 - 7).
	uint16_t mix_len = 2;			  // the max length for the CJK words in a mix string.
	uint16_t clr_stw = 0;			  // clear the stopwords.
	uint16_t keep_urec = 0;			  // keep the unrecognized words.
	char kpuncs[kKeepPuncLen] = {0};  // keep punctuations buffer.
};

class [[nodiscard]] EqualF {
public:
	using is_transparent = void;
	template <typename T1, typename T2>
	bool operator()(const T1& v1, const T2& v2) const {
		return equal(v1, v2);
	}

private:
	bool equal(const WordInf& v1, const WordInf& v2) const noexcept { return v1.word == v2.word; }
	bool equal(std::string_view v1, const WordInf& v2) const noexcept { return v1 == v2.word; }
	bool equal(const WordInf& v1, std::string_view v2) const noexcept { return v1.word == v2; }
};

class [[nodiscard]] LessF {
public:
	using is_transparent = void;
	template <typename T1, typename T2>
	bool operator()(const T1& v1, const T2& v2) const {
		return less(v1, v2);
	}

private:
	bool less(const WordInf& v1, const WordInf& v2) const noexcept { return v1.word < v2.word; }
	bool less(std::string_view v1, const WordInf& v2) const noexcept { return v1 < v2.word; }
	bool less(const WordInf& v1, std::string_view v2) const noexcept { return v1.word < v2; }
};

class [[nodiscard]] HashF {
public:
	using is_transparent = void;
	template <typename T1>
	size_t operator()(const T1& v) const noexcept {
		return hash(v);
	}

private:
	size_t hash(const WordInf& v) const noexcept { return hash_str()(v.word); }
	size_t hash(const std::string& v) const noexcept { return hash_str()(v); }
	size_t hash(std::string_view v) const noexcept { return hash_str()(v); }
};

class [[nodiscard]] Dictionary {
public:
	using DictTable = fast_hash_set<WordInf, HashF, EqualF, LessF>;
	Dictionary();
	const DictTable& operator[](unsigned indx) const;

private:
	struct [[nodiscard]] LexFileInfo {
		FrisoLexType type;
		std::string name;
	};

	void loadDictFromFiles();
	std::vector<std::string> splitSyn(const std::string& s, const std::string& delimiter);
	void parseDictPart(WordInf& rec, int counter, const std::string& data);
	void loadOneFile(const LexFileInfo& fInf, DictTable& loadDict);

	std::array<DictTable, __LEX_MAX> dict_;
};

class FrisoTextSplitter;

struct [[nodiscard]] FrisoChunkEntry {
	h_vector<const WordInf*, 3> words;
	uint32_t length = 0;
	float average_word_length = 0.0;
	float word_length_variance = 0.0;
	float single_word_dmf = 0.0;
};

class [[nodiscard]] FrisoTask final : public ISplitterTask {
public:
	void SetText(std::string_view t) noexcept override {
		str = t;
		words.clear();
		wordsOffset.clear();
		idx = 0;
		charCounter = 0;
		utf8Buffer_.clear();
		unicode = 0;
		pool.clear();
		convertCounter = 0;
	}
	const std::vector<WordWithPos>& GetResults() override {
		FrisoTokenEntry token;
		while (next_mmseg_token(token)) {
			wordsOffset.emplace_back(token);
		}
		for (const auto& tok : wordsOffset) {
			words.emplace_back(tokenWord(tok), words.size());
		}
		return words;
	}
	std::pair<int, int> Convert(unsigned int wordPosStart, unsigned int wordPosEnd) override;

	void WordToByteAndCharPos(int wordPosition, WordPosition& out) override;
	void WordToByteAndCharPos(int wordPosition, WordPositionEx& out) override;

	friend class FrisoTextSplitter;

private:
	FrisoTask(const FrisoTextSplitter& s) noexcept : splitter_(std::move(s)) {}

	const unsigned int kHitsWordLen = 64;
	std::string_view str;  // text to tokenize

	std::vector<WordWithPos> words;
	std::vector<FrisoTokenEntry> wordsOffset;

	const FrisoTextSplitter& splitter_;
	size_t idx = 0;	 // start offset index.
	size_t charCounter = 0;

	std::string utf8Buffer_;	  // latest symbol in utf8
	uint16_t unicode = 0;		  // latest symbol unicode number.
	elist<FrisoTokenEntry> pool;  // task pool.
	StringBuffer wordStorage;

	unsigned int convertCounter = 0;

	bool next_mmseg_token(FrisoTokenEntry& token);
	void get_next_match(size_t _idx, std::vector<const WordInf*>& match);
	const WordInf* next_complex_cjk();

	FrisoTokenEntry next_basic_latin();

	FrisoChunkEntry mmseg_core_invoke(std::vector<FrisoChunkEntry>& chunks);

	int utf8_next_word(size_t* idx);

	float count_chunk_avl(FrisoChunkEntry& chunk);
	float count_chunk_var(FrisoChunkEntry& chunk);
	float count_chunk_mdf(FrisoChunkEntry& chunk);

	std::string_view tokenWord(const FrisoTokenEntry& e) const;

	bool convert_full_to_half(uint16_t& c);
	bool convert_upper_to_lower(uint16_t& c);
	/* convert the unicode to utf-8 bytes. (FRISO_UTF8) */
	void convert_work_apply(bool& convert);
};

class [[nodiscard]] FrisoTextSplitter final : public ISplitter {
public:
	FrisoTextSplitter() = default;
	~FrisoTextSplitter() = default;

	std::shared_ptr<ISplitterTask> CreateTask() const override;
	const Dictionary& Dict() const noexcept { return dict_; }
	const FrisoConfig& Config() const noexcept { return config_; }

private:
	Dictionary dict_;
	FrisoConfig config_;
};

}  // namespace reindexer
