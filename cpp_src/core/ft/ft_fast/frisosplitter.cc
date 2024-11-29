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

#include "frisosplitter.h"
#include <sstream>
#include "../_cmrc/include/cmrc/cmrc.hpp"
#include "frisochartypes.h"
#include "tools/stringstools.h"
#include "vendor/utf8cpp/utf8/unchecked.h"

static void initResources() { CMRC_INIT(friso_dict_resources); }

namespace reindexer {

Dictionary::Dictionary() {
	initResources();
	loadDictFromFiles();
}

const Dictionary::DictTable& Dictionary::operator[](unsigned indx) const {
	assertrx(indx < dict_.size());
	return dict_[indx];
}

std::vector<std::string> Dictionary::splitSyn(const std::string& s, const std::string& delimiter) {
	std::vector<std::string> tokens;
	size_t pos = 0;
	std::string token;
	size_t startPos = 0;
	while ((pos = s.find(delimiter, startPos)) != std::string::npos) {
		token = s.substr(startPos, pos);
		tokens.push_back(token);
		startPos = pos + delimiter.size();
	}
	tokens.push_back(s.substr(startPos));

	return tokens;
}

void Dictionary::parseDictPart(WordInf& rec, int counter, const std::string& data) {
	switch (counter) {
		case 0:
			rec.word = data;
			for (unsigned idx = 0; idx < rec.word.size();) {
				const char* it = &rec.word[idx];
				const char* start = &rec.word[idx];
				utf8::unchecked::next(it);
				idx += it - start;
				rec.wordChars++;
			}
			break;
		case 1:
			if (data != "null") {
				rec.syn = splitSyn(data, ",");
			}
			break;
		case 2:
			rec.fre = std::stoi(data);
			break;
		default:
			assertrx_throw(false);
	}
}

void Dictionary::loadOneFile(const LexFileInfo& fInf, DictTable& loadDict) {
	auto dictRes = cmrc::open(fInf.name);
	if (dictRes.begin() == nullptr) {
		throw reindexer::Error(errLogic, "Incorrect friso dictionary resource.");
	}
	std::string s(std::string_view(dictRes.begin(), std::distance(dictRes.begin(), dictRes.end())));
	std::stringstream fp{s, std::ios::in};
	std::string line;
	while (std::getline(fp, line)) {
		// one # is possible in lex-stopword.lex
		if (line.size() > 1 && line[0] == '#') {
			continue;
		}
		size_t posStart = 0;
		size_t counter = 0;
		WordInf dictRecord;
		dictRecord.type = fInf.type;
		for (size_t pos = line.find('/'); pos != std::string::npos; pos = line.find('/', posStart), counter++) {
			parseDictPart(dictRecord, counter, line.substr(posStart, pos));
			posStart = pos + 1;
		}
		parseDictPart(dictRecord, counter, line.substr(posStart));
		loadDict.insert(dictRecord);
	}
}

void Dictionary::loadDictFromFiles() {
	const std::vector<LexFileInfo> fileInfos = {
		{__LEX_CJK_WORDS__, "china_dict/lex-main.lex"},		{__LEX_CJK_WORDS__, "china_dict/lex-admin.lex"},
		{__LEX_CJK_WORDS__, "china_dict/lex-chars.lex"},	{__LEX_CJK_WORDS__, "china_dict/lex-cn-mz.lex"},
		{__LEX_CJK_WORDS__, "china_dict/lex-cn-place.lex"}, {__LEX_CJK_WORDS__, "china_dict/lex-company.lex"},
		{__LEX_CJK_WORDS__, "china_dict/lex-festival.lex"}, {__LEX_CJK_WORDS__, "china_dict/lex-flname.lex"},
		{__LEX_CJK_WORDS__, "china_dict/lex-food.lex"},		{__LEX_CJK_WORDS__, "china_dict/lex-lang.lex"},
		{__LEX_CJK_WORDS__, "china_dict/lex-nation.lex"},	{__LEX_CJK_WORDS__, "china_dict/lex-net.lex"},
		{__LEX_CJK_WORDS__, "china_dict/lex-org.lex"},		{__LEX_CJK_WORDS__, "china_dict/lex-touris.lex"},

		{__LEX_CJK_UNITS__, "china_dict/lex-units.lex"},	{__LEX_ECM_WORDS__, "china_dict/lex-ecmixed.lex"},
		{__LEX_CEM_WORDS__, "china_dict/lex-cemixed.lex"},	{__LEX_CN_LNAME__, "china_dict/lex-lname.lex"},
		{__LEX_CN_SNAME__, "china_dict/lex-sname.lex"},		{__LEX_CN_LNA__, "china_dict/lex-ln-adorn.lex"},
		{__LEX_STOPWORDS__, "china_dict/lex-stopword.lex"},
	};

	for (auto& fInfo : fileInfos) {
		loadOneFile(fInfo, dict_[fInfo.type]);
	}
}

std::pair<int, int> FrisoTask::Convert(unsigned int startWord, unsigned int endWord) {
	FrisoTokenEntry token;
	std::pair<int, int> ret(0, 0);
	while (next_mmseg_token(token)) {
		convertCounter++;
		if (convertCounter == startWord + 1) {
			ret.first = token.GetInOrigin().offset;
		}
		if (convertCounter == endWord) {
			WordPos p = token.GetInOrigin();
			ret.second = p.offset + p.length;
			break;
		}
	}
	return ret;
}

int FrisoTask::utf8_next_word(size_t* idx_) {
	if (*idx_ >= str.length()) {
		return 0;
	}

	const char* it = &str[*idx_];
	const char* start = it;
	unicode = utf8::unchecked::next(it);
	unsigned int bytes = it - start;

	utf8Buffer_.assign(&str[*idx_], bytes);
	(*idx_) += bytes;

	return bytes;
}

void FrisoTask::WordToByteAndCharPos(int wordPosition, WordPosition& out) {
	int tokenCounter = 0;
	FrisoTokenEntry token;
	while (next_mmseg_token(token)) {
		if (wordPosition == tokenCounter) {
			WordPos wb = token.GetInOrigin();
			out.SetBytePosition(wb.offset, wb.offset + wb.length);
			break;
		}
		tokenCounter++;
	}
}
void FrisoTask::WordToByteAndCharPos(int wordPosition, WordPositionEx& out) {
	int tokenCounter = 0;
	FrisoTokenEntry token;
	while (next_mmseg_token(token)) {
		if (wordPosition == tokenCounter) {
			WordPos ws = token.GetInSymbols();
			out.start.ch = ws.offset;
			out.end.ch = ws.offset + ws.length;

			WordPos wb = token.GetInOrigin();
			out.SetBytePosition(wb.offset, wb.offset + wb.length);
			break;
		}
		tokenCounter++;
	}
}

bool FrisoTask::next_mmseg_token(FrisoTokenEntry& token) {
	// task word pool check
	if (!pool.empty()) {
		/*
		 * load word from the word pool if it is not empty.
		 *  this will make the next word more convenient and efficient.
		 *     often synonyms, newly created word will be stored in the pool.
		 */
		token = pool.front();
		pool.pop_front();
		return true;
	}

	while (idx < str.length()) {
		// read the next symbol from the current position.
		unsigned int bytes = utf8_next_word(&idx);
		if (bytes == 0) {
			break;
		}
		charCounter++;
		// clear up the whitespace.
		if (FrisoCharTypes::utf8_whitespace(unicode)) {
			continue;
		}

		// CJK words recongnize block.
		if (FrisoCharTypes::utf8_cjk_string(unicode)) {
			/* check the dictionary.
			 * and return the unrecognized CJK char as a single word.
			 * */
			if (splitter_.Dict()[__LEX_CJK_WORDS__].find(utf8Buffer_) == splitter_.Dict()[__LEX_CJK_WORDS__].end()) {
				wordStorage.NewWord();
				wordStorage.AddString(utf8Buffer_);
				token = FrisoTokenEntry{__LEX_PUNC_WORDS__,
										{wordStorage.LastWordSize(), wordStorage.LastWordOffset()},
										{bytes, idx - bytes},
										{1, charCounter - 1}};
				return true;
			}

			const WordInf* wordInf = next_complex_cjk();

			if (!wordInf) {
				continue;  // find a stopwrod.
			}
			charCounter--;
			wordStorage.NewWord();
			wordStorage.AddString(wordInf->word);

			token = FrisoTokenEntry{wordInf->type,
									{wordStorage.LastWordSize(), wordStorage.LastWordOffset()},
									{wordInf->word.size(), idx - wordInf->word.size()},
									{wordInf->wordChars, charCounter}};
			charCounter += wordInf->wordChars;

			/*
			 * try to find a chinese and english mixed words, like '卡拉ok'
			 *     keep in mind that is not english and chinese mixed words
			 *         like 'x射线'.
			 *
			 * @reader:
			 * 1. only if the char after the current word is an english char.
			 * 2. if the first point meet, friso will call next_basic_latin() to
			 *         get the next basic latin. (yeah, you have to handle it).
			 * 3. if match a CE word, set lex to the newly match CE word.
			 * 4. if no match a CE word, we will have to append the basic latin
			 *         to the pool, and it should after the append of synonyms words.
			 * 5. do not use the task->buffer and task->unicode as the check
			 *         condition for the CE word identify.
			 * 6. Add friso_numeric_letter check so can get work like '高3'
			 *
			 * @date 2013-09-02
			 */

			FrisoTokenEntry tmp;
			int isFindCem = -1;
			if ((idx < str.length()) && (int(str[idx])) > 0 &&
				(FrisoCharTypes::utf8_en_letter(str[idx]) || FrisoCharTypes::utf8_numeric_letter(str[idx]))) {
				size_t cemStart = wordStorage.LastWordOffset();
				size_t cemLen = wordStorage.LastWordSize();

				// find the next basic latin.
				utf8Buffer_ = str[idx];
				idx++;
				tmp = next_basic_latin();

				isFindCem = 0;
				size_t cemWordSize = cemLen + tmp.GetInStorage().length;
				std::string_view cemWord = wordStorage.GetString(cemStart, cemWordSize);

				// check the CE dictionary.
				auto findCemWords = splitter_.Dict()[__LEX_CEM_WORDS__].find(cemWord);
				if (findCemWords != splitter_.Dict()[__LEX_CEM_WORDS__].end()) {
					token = FrisoTokenEntry{__LEX_CEM_WORDS__,
											{cemWordSize, cemStart},
											{token.GetInOrigin().length + tmp.GetInOrigin().length, token.GetInOrigin().offset},
											{token.GetInSymbols().length + tmp.GetInSymbols().length, token.GetInSymbols().offset}};
					isFindCem = 1;
				}
			}
			if (isFindCem == 0) {
				pool.push_back(tmp);
			}
			return true;
		}

		// basic english/latin recongnize block.
		else if (FrisoCharTypes::utf8_halfwidth_en_char(unicode) || FrisoCharTypes::utf8_fullwidth_en_char(unicode)) {
			/*
			 * handle the english punctuation.
			 *
			 * @todo:
			 * 1. comment all the code of the following if
			 *     and uncomment the continue to clear up the punctuation directly.
			 *
			 * @reader:
			 * 2. keep in mind that ALL the english punctuation will be handled here,
			 *  (when a english punctuation is found during the other process, we will
			 *      reset the task->idx back to it and then back here)
			 *     except the keep punctuation(define in file friso_string.c)
			 *     that will make up a word with the english chars around it.
			 */
			if (FrisoCharTypes::utf8_en_punctuation(unicode)) {
				if (splitter_.Config().clr_stw &&
					splitter_.Dict()[__LEX_STOPWORDS__].find(utf8Buffer_) != splitter_.Dict()[__LEX_STOPWORDS__].end()) {
					continue;
				}
				wordStorage.NewWord();
				wordStorage.AddString(utf8Buffer_);
				token = FrisoTokenEntry{__LEX_PUNC_WORDS__,
										{wordStorage.LastWordSize(), wordStorage.LastWordOffset()},
										{bytes, idx - bytes},
										{1, charCounter - 1}};
				return true;
			}

			// get the next basic latin word.
			token = next_basic_latin();

			// check if it is a stopword.
			if (splitter_.Config().clr_stw &&
				splitter_.Dict()[__LEX_STOPWORDS__].find(tokenWord(token)) != splitter_.Dict()[__LEX_STOPWORDS__].end()) {
				continue;
			}

			return true;
		}
		// Keep the chinese punctuation.
		else if (FrisoCharTypes::utf8_cn_punctuation(unicode)) {
			if (splitter_.Config().clr_stw &&
				splitter_.Dict()[__LEX_STOPWORDS__].find(utf8Buffer_) != splitter_.Dict()[__LEX_STOPWORDS__].end()) {
				continue;
			}

			// count the punctuation in.
			// memcpy(task.token.word, task.buffer, task.bytes);
			wordStorage.NewWord();
			wordStorage.AddString(utf8Buffer_);
			token = FrisoTokenEntry{
				__LEX_PUNC_WORDS__, {wordStorage.LastWordSize(), wordStorage.LastWordOffset()}, {bytes, idx - bytes}, {1, charCounter - 1}};
			return true;
		}
		// keep the unrecognized words?
		else if (splitter_.Config().keep_urec) {
			wordStorage.NewWord();
			wordStorage.AddString(utf8Buffer_);
			token = FrisoTokenEntry{__LEX_UNKNOW_WORDS__,
									{wordStorage.LastWordSize(), wordStorage.LastWordOffset()},
									{
										bytes,
										idx - bytes,
									},
									{1, charCounter - 1}};
			return true;
		}
	}

	return false;
}

const WordInf* FrisoTask::next_complex_cjk() {
	/*backup the task->bytes here*/
	unsigned long lastSymbolBytes = utf8Buffer_.size();

	std::vector<const WordInf*> fmatch;
	get_next_match(idx, fmatch);

	/*
	 * here:
	 *        if the length of the fmatch is 1, mean we don't have to
	 * continue the following work. ( no matter what we get the same result. )
	 */
	if (fmatch.size() == 1) {
		const WordInf* fe = fmatch[0];

		/*
		 * check and clear the stop words .
		 * @date 2013-06-13
		 */
		if (splitter_.Config().clr_stw && splitter_.Dict()[__LEX_STOPWORDS__].find(fe->word) != splitter_.Dict()[__LEX_STOPWORDS__].end()) {
			return nullptr;
		}

		return fe;
	}

	std::vector<FrisoChunkEntry> chunks;
	idx -= lastSymbolBytes;

	for (size_t x = 0; x < fmatch.size(); x++) {
		/*get the word and try the second layer match*/
		const WordInf* fe = fmatch[x];
		size_t idxLocal = idx + fe->word.size();
		unsigned int bytes = utf8_next_word(&idxLocal);
		if (bytes != 0 && FrisoCharTypes::utf8_cjk_string(unicode) &&
			splitter_.Dict()[__LEX_CJK_WORDS__].find(utf8Buffer_) != splitter_.Dict()[__LEX_CJK_WORDS__].end()) {
			// get the next matches
			std::vector<const WordInf*> smatch;
			get_next_match(idxLocal, smatch);
			for (size_t y = 0; y < smatch.size(); y++) {
				/*get the word and try the third layer match*/
				const WordInf* se = smatch[y];
				idxLocal = idx + fe->word.size() + se->word.size();
				bytes = utf8_next_word(&idxLocal);

				if (bytes != 0 && FrisoCharTypes::utf8_cjk_string(unicode) &&
					splitter_.Dict()[__LEX_CJK_WORDS__].find(utf8Buffer_) != splitter_.Dict()[__LEX_CJK_WORDS__].end()) {
					// get the matchs.
					std::vector<const WordInf*> tmatch;
					get_next_match(idxLocal, tmatch);
					for (size_t z = 0; z < tmatch.size(); z++) {
						const WordInf* te = tmatch[z];
						FrisoChunkEntry chunk;
						chunk.words.resize(3);
						chunk.words[0] = fe;
						chunk.words[1] = se;
						chunk.words[2] = te;

						chunk.length = fe->word.size() + se->word.size() + te->word.size();
						chunks.push_back(chunk);
					}
				} else {
					FrisoChunkEntry chunk;
					chunk.words.resize(2);
					chunk.words[0] = fe;
					chunk.words[1] = se;
					chunk.length = fe->word.size() + se->word.size();
					chunks.push_back(chunk);
				}
			}
		} else {
			FrisoChunkEntry chunk;
			chunk.words.resize(1);
			chunk.words[0] = fe;
			chunk.length = fe->word.size();
			chunks.push_back(chunk);
		}
	}

	/*
	 * filter the chunks with the four rules of the mmseg algorithm
	 *        and get best chunk as the final result.
	 *
	 * @see mmseg_core_invoke( chunks );
	 * @date 2012-12-13
	 */
	FrisoChunkEntry e;
	if (chunks.size() > 1) {
		e = mmseg_core_invoke(chunks);
	} else {
		e = chunks[0];
	}

	const WordInf* fe = e.words[0];
	idx += fe->word.size();	 // reset the idx of the task.

	// clear the stop words
	if (splitter_.Config().clr_stw && splitter_.Dict()[__LEX_STOPWORDS__].find(fe->word) != splitter_.Dict()[__LEX_STOPWORDS__].end()) {
		return nullptr;
	}

	return fe;
}

void FrisoTask::get_next_match(size_t idx_, std::vector<const WordInf*>& match) {
	size_t wordStart = idx_ - utf8Buffer_.size();
	size_t wordLen = utf8Buffer_.size();

	auto it = splitter_.Dict()[__LEX_CJK_WORDS__].find(std::string_view(&str[0] + wordStart, wordLen));
	if (it != splitter_.Dict()[__LEX_CJK_WORDS__].end()) {
		match.push_back(&*it);
	}

	unsigned int bytes = 0;
	for (unsigned int t = 1; t < splitter_.Config().max_len && (bytes = utf8_next_word(&idx_)) != 0; t++) {
		if (FrisoCharTypes::utf8_whitespace(unicode)) {
			break;
		}
		if (!FrisoCharTypes::utf8_cjk_string(unicode)) {
			break;
		}
		wordLen += bytes;
		auto its = splitter_.Dict()[__LEX_CJK_WORDS__].find(std::string_view(&str[0] + wordStart, wordLen));
		if (its != splitter_.Dict()[__LEX_CJK_WORDS__].end()) {
			match.push_back(&(*its));
		}
	}
}

FrisoChunkEntry FrisoTask::mmseg_core_invoke(std::vector<FrisoChunkEntry>& chunks) {
	unsigned int t;
	float max;
	std::vector<FrisoChunkEntry> __res__;
	__res__.reserve(chunks.size());
	std::vector<FrisoChunkEntry> __tmp__;

	// 1.get the maximum matched chunks.
	// count the maximum length
	max = chunks[0].length;
	for (t = 1; t < chunks.size(); t++) {
		FrisoChunkEntry& e = chunks[t];
		if (e.length > max) {
			max = float(e.length);
		}
	}
	// get the chunk items that owns the maximum length.
	for (t = 0; t < chunks.size(); t++) {
		FrisoChunkEntry& e = chunks[t];
		if (e.length >= max) {
			__res__.push_back(e);
		}
	}
	// check the left chunks
	if (__res__.size() == 1) {
		FrisoChunkEntry& e = __res__[0];
		return e;
	} else {
		chunks = __res__;
		__res__.clear();
	}

	// 2.get the largest average word length chunks.
	// count the maximum average word length.
	max = count_chunk_avl(chunks[0]);
	for (t = 1; t < chunks.size(); t++) {
		FrisoChunkEntry& e = chunks[t];
		if (count_chunk_avl(e) > max) {
			max = e.average_word_length;
		}
	}
	// get the chunks items that own the largest average word length.
	for (t = 0; t < chunks.size(); t++) {
		FrisoChunkEntry& e = chunks[t];
		if (e.average_word_length >= max) {
			__res__.push_back(e);
		}
	}
	// check the left chunks
	if (__res__.size() == 1) {
		FrisoChunkEntry& e = __res__[0];
		return e;
	} else {
		chunks = __res__;
		__res__.clear();
	}

	// 3.get the smallest word length variance chunks
	// count the smallest word length variance
	max = count_chunk_var(chunks[0]);
	for (t = 1; t < chunks.size(); t++) {
		FrisoChunkEntry& e = chunks[t];
		if (count_chunk_var(e) < max) {
			max = e.word_length_variance;
		}
	}
	// get the chunks that own the smallest word length variance.
	for (t = 0; t < chunks.size(); t++) {
		FrisoChunkEntry& e = chunks[t];
		if (e.word_length_variance <= max) {
			__res__.push_back(e);
		} else {
			e.words.clear();  //????
		}
	}
	// check the left chunks
	if (__res__.size() == 1) {
		FrisoChunkEntry& e = __res__[0];
		return e;
	} else {
		chunks = __res__;
		__res__.clear();
	}

	// 4.get the largest single word morpheme degrees of freedom.
	// count the maximum single word morpheme degrees of freedom
	max = count_chunk_mdf(chunks[0]);
	for (t = 1; t < chunks.size(); t++) {
		FrisoChunkEntry& e = chunks[t];
		if (count_chunk_mdf(e) > max) {
			max = e.single_word_dmf;
		}
	}
	// get the chunks that own the largest single word word morpheme degrees of freedom.
	for (t = 0; t < chunks.size(); t++) {
		FrisoChunkEntry& e = chunks[t];
		if (e.single_word_dmf >= max) {
			__res__.push_back(e);
		} else {
			e.words.clear();
		}
	}

	/*
	 * there is still more than one chunks?
	 *        well, this rarely happen but still happens.
	 * here we simple return the first chunk as the final result,
	 *         and we need to free the all the chunks that __res__
	 *     points to except the 1sh one.
	 * you have to do two things to totally free a chunk:
	 * 1. call free_array_list to free the allocations of a chunk's words.
	 * 2. call free_chunk to the free the allocations of a chunk.
	 */

	chunks.clear();

	return __res__[0];
}

FrisoTokenEntry FrisoTask::next_basic_latin() {
	bool chkEnglishChinaMixed = false;	// flag english china mixed
	int chkunits = 1, wspace = 0;

	/* cause friso will convert full-width numeric and letters
	 *     (Not punctuations) to half-width ones. so, here we need
	 * wlen to record the real length of the lex_entry_t.
	 * */
	const WordInf* wInf = nullptr;

	unsigned long charBytes = utf8Buffer_.size();

	// full-half width and upper-lower case exchange.
	bool convert = false;
	convert |= convert_full_to_half(unicode);
	convert |= convert_upper_to_lower(unicode);
	convert_work_apply(convert);

	// creat a new fstring buffer and append the task->buffer insite.
	wordStorage.NewWord();
	wordStorage.AddString(utf8Buffer_);

	unsigned long charCount = 1;

	unsigned int bytes = 0;
	while ((bytes = utf8_next_word(&idx)) != 0) {
		unsigned int bytesRead = bytes;
		// convert full-width to half-width.
		convert |= convert_full_to_half(unicode);
		friso_enchar_t _ctype = FrisoCharTypes::friso_enchar_type(unicode);

		if (_ctype == FRISO_EN_WHITESPACE) {
			wspace = 1;
			idx -= bytes;
			break;
		}

		if (_ctype == FRISO_EN_PUNCTUATION) {
			// clear the full-width punctuations.
			if (bytes > 1) {
				idx -= bytes;
				break;
			}
			if (!FrisoCharTypes::friso_en_kpunc(splitter_.Config().kpuncs, utf8Buffer_[0])) {
				idx -= bytes;
				break;
			}
		}

		/* check if is an FRISO_EN_NUMERIC, or FRISO_EN_LETTER.
		 *     here just need to make sure it is not FRISO_EN_UNKNOW.
		 * */
		if (_ctype == FRISO_EN_UNKNOWN) {
			if (FrisoCharTypes::utf8_cjk_string(unicode)) {
				chkEnglishChinaMixed = true;
			}
			idx -= bytes;
			break;
		}

		// upper-lower case convert
		convert |= convert_upper_to_lower(unicode);
		convert_work_apply(convert);

		// sound a little crazy, i did't limit the length of this
		//@Added: 2015-01-16 night
		if ((wordStorage.LastWordSize() + bytes) >= kHitsWordLen) {
			break;
		}
		charCount++;
		charCounter++;
		charBytes += bytesRead;
		wordStorage.AddString(utf8Buffer_);
	}

	// check the tokenize loop is break by whitespace.
	//     no need for all the following work if it is.
	//@added 2013-11-19
	if (wspace == 1 || idx == str.length()) {
		FrisoTokenEntry e{__LEX_OTHER_WORDS__,
						  {wordStorage.LastWordSize(), wordStorage.LastWordOffset()},
						  {charBytes, idx - charBytes},
						  {charCount, charCounter - charCount}};
		return e;
	}

	if (!chkEnglishChinaMixed) {
		/*
		 * check the single words unit.
		 *     not only the chinese word but also other kinds of word.
		 * so we can recognize the complex unit like '℉,℃'' eg..
		 * @date 2013-10-14
		 */
		if (chkunits &&
			(FrisoCharTypes::utf8_numeric_string(wordStorage.LastWord()) || FrisoCharTypes::utf8_decimal_string(wordStorage.LastWord()))) {
			if ((bytes = utf8_next_word(&idx)) != 0) {
				// check the EC dictionary.
				if (splitter_.Dict()[__LEX_CJK_UNITS__].find(utf8Buffer_) != splitter_.Dict()[__LEX_CJK_UNITS__].end()) {
					wordStorage.AddString(utf8Buffer_);
					charBytes += bytes;
					charCount++;
					charCounter++;
				} else {
					idx -= bytes;
				}
			}
		}

		FrisoTokenEntry e{__LEX_OTHER_WORDS__,
						  {wordStorage.LastWordSize(), wordStorage.LastWordOffset()},
						  {charBytes, idx - charBytes},
						  {charCount, charCounter - charCount}};
		return e;
	}

	// Try to find a english chinese mixed word.
	size_t lastActualWordOffset = wordStorage.LastWordOffset();
	size_t lastActualWordSize = wordStorage.LastWordSize();
	size_t lastWordSize = 0;
	size_t tmpIdx = idx;
	size_t tmpCharCount = 0;
	size_t tmpBytesCounter = 0;
	size_t tmpBytes = 0;
	size_t tmpIndxWord = idx;
	for (int t = 0; t < splitter_.Config().mix_len && (bytes = utf8_next_word(&tmpIdx)) != 0; t++) {
		// replace with the whitespace check.
		// more complex mixed words could be find here.
		//  (no only english and chinese mix word)
		//@date 2013-10-14
		if (FrisoCharTypes::utf8_whitespace(unicode)) {
			break;
		}

		wordStorage.AddString(utf8Buffer_);
		tmpBytesCounter += bytes;

		auto it = splitter_.Dict()[__LEX_ECM_WORDS__].find(wordStorage.LastWord());
		if (it != splitter_.Dict()[__LEX_ECM_WORDS__].end()) {
			wInf = &(*it);
			lastWordSize = wordStorage.LastWordSize();
			tmpCharCount = t + 1;
			tmpBytes = tmpBytesCounter;
			tmpIndxWord = tmpIdx;
		}
	}

	if (lastWordSize) {
		charCount += tmpCharCount;
		charCounter += tmpCharCount;
		idx = tmpIndxWord;
		charBytes += tmpBytes;
		FrisoTokenEntry e{wInf->type, {lastWordSize, lastActualWordOffset}, {charBytes, idx - charBytes}, {charCount, charCounter}};
		return e;
	}

	wordStorage.Resize(lastActualWordOffset + lastActualWordSize);	// reset CEM word in buffer
	// no match for mix word, try to find a single unit.
	if (chkunits &&
		(FrisoCharTypes::utf8_numeric_string(wordStorage.LastWord()) || FrisoCharTypes::utf8_decimal_string(wordStorage.LastWord()))) {
		if ((bytes = utf8_next_word(&idx)) != 0) {
			// check the single chinese units dictionary.
			if (splitter_.Dict()[__LEX_CJK_UNITS__].find(utf8Buffer_) != splitter_.Dict()[__LEX_CJK_UNITS__].end()) {
				wordStorage.AddString(utf8Buffer_);	 // sb += task.buffer;
				charBytes += bytes;
				charCount++;
				charCounter++;
			} else {
				idx -= bytes;
			}
		}
	}

	FrisoTokenEntry e{__LEX_OTHER_WORDS__,
					  {wordStorage.LastWordSize(), wordStorage.LastWordOffset()},
					  {charBytes, idx - charBytes},
					  {charCount, charCounter - charCount}};
	return e;
}

float FrisoTask::count_chunk_avl(FrisoChunkEntry& chunk) {
	chunk.average_word_length = (float(chunk.length)) / chunk.words.size();
	return chunk.average_word_length;
}
float FrisoTask::count_chunk_var(FrisoChunkEntry& chunk) {
	float var = 0;	// snapshot
	unsigned int t;

	for (t = 0; t < chunk.words.size(); t++) {
		const WordInf* e = chunk.words[t];
		float tmp = e->word.size() - chunk.average_word_length;
		var += tmp * tmp;
	}

	chunk.word_length_variance = var / chunk.words.size();

	return chunk.word_length_variance;
}

float FrisoTask::count_chunk_mdf(FrisoChunkEntry& chunk) {
	float __mdf__ = 0;

	for (size_t t = 0; t < chunk.words.size(); t++) {
		const WordInf* e = chunk.words[t];
		// single CJK(UTF-8)/chinese(GBK) word.
		// better add a charset check here, but this will works find.
		// all CJK words will take 3 bytes with UTF-8 encoding.
		// all chinese words take 2 bytes with GBK encoding.
		if (e->word.size() == 3 || e->word.size() == 2) {
			__mdf__ += float(std::log(float(e->fre)));
		}
	}
	chunk.single_word_dmf = __mdf__;

	return chunk.single_word_dmf;
}

std::string_view FrisoTask::tokenWord(const FrisoTokenEntry& e) const {
	WordPos t = e.GetInStorage();
	return wordStorage.GetString(t.offset, t.length);
}
bool FrisoTask::convert_full_to_half(uint16_t& c) {
	if (FrisoCharTypes::utf8_fullwidth_en_char(c)) {
		c -= 65248;
		return true;
	}
	return false;
}

bool FrisoTask::convert_upper_to_lower(uint16_t& c) {
	if (FrisoCharTypes::utf8_uppercase_letter(c)) {
		c += 32;
		return true;
	}
	return false;
}

void FrisoTask::convert_work_apply(bool& convert) {
	if (convert) {
		utf8Buffer_.clear();
		utf8::unchecked::append(unicode, std::back_inserter(utf8Buffer_));
		convert = false;
	}
}

std::shared_ptr<ISplitterTask> FrisoTextSplitter::CreateTask() const { return std::make_shared<FrisoTask>(FrisoTask(*this)); }

}  // namespace reindexer
