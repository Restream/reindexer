
#include "typos.h"

namespace reindexer {

void mktypos(const std::wstring& word, size_t maxTyposInWord, uint8_t maxTyposLen, const TyposCallBack& callback, std::wstring& buf) {
	// not supported
	if (maxTyposInWord > 2) [[unlikely]] {
		throw Error(errLogic, "Unexpected maxTyposInWord value for mktypo(): {}", maxTyposInWord);
	}

	callback(word, TyposVec(), word);
	if (maxTyposInWord == 0 || word.length() < 3 || word.length() > maxTyposLen) {
		return;
	}

	std::wstring& wordWith1MissingLetter = buf;
	wordWith1MissingLetter.assign(word.data() + 1, word.size() - 1);
	for (uint8_t i = 0;; ++i) {
		callback(wordWith1MissingLetter, TyposVec(i), word);
		if (i >= wordWith1MissingLetter.length()) {
			break;
		}
		wordWith1MissingLetter[i] = word[i];
	}

	if (maxTyposInWord == 2 && word.length() > 3) {
		std::wstring& wordWith2MissingLetters = buf;
		for (unsigned i = 0; i + 1 < word.size(); ++i) {
			wordWith2MissingLetters.resize(i);
			for (unsigned j = i + 2; j < word.size(); ++j) {
				wordWith2MissingLetters.push_back(word[j]);
			}

			for (unsigned j = i;; ++j) {
				callback(wordWith2MissingLetters, TyposVec(i, j + 1), word);
				if (j >= wordWith2MissingLetters.length()) {
					break;
				}
				wordWith2MissingLetters[j] = word[j + 1];
			}

			wordWith2MissingLetters[i] = word[i];
		}
	}
}

}  // namespace reindexer
