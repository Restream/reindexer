#include "splitter.h"
#include "tools/stringstools.h"
#include "vendor/utf8cpp/utf8/unchecked.h"

namespace reindexer {

static std::pair<int, int> word2Pos(std::string_view str, int wordPos, int endPos, const SplitOptions& splitOptions) {
	auto wordStartIt = str.begin();
	auto wordEndIt = str.begin();
	auto it = str.begin();
	auto endIt = str.end();
	assertrx(endPos > wordPos);
	int numWords = endPos - (wordPos + 1);
	for (; it != endIt;) {
		auto ch = utf8::unchecked::next(it);

		while (it != endIt && !splitOptions.IsWordSymbol(ch)) {
			wordStartIt = it;
			ch = utf8::unchecked::next(it);
		}

		while (splitOptions.IsWordSymbol(ch)) {
			wordEndIt = it;
			if (it == endIt) {
				break;
			}
			ch = utf8::unchecked::next(it);
		}

		if (wordStartIt != it) {
			if (!wordPos) {
				break;
			} else {
				wordPos--;
				wordStartIt = it;
			}
		}
	}

	for (; numWords != 0 && it != endIt; numWords--) {
		auto ch = utf8::unchecked::next(it);

		while (it != endIt && !IsAlpha(ch) && !IsDigit(ch)) {
			ch = utf8::unchecked::next(it);
		}

		while (splitOptions.IsWordSymbol(ch)) {
			wordEndIt = it;
			if (it == endIt) {
				break;
			}
			ch = utf8::unchecked::next(it);
		}
	}

	return {int(std::distance(str.begin(), wordStartIt)), int(std::distance(str.begin(), wordEndIt))};
}

ISplitterTask::~ISplitterTask() = default;

const std::vector<WordWithPos>& SplitterTaskFast::GetResults() {
	split(text_, convertedText_, words_, splitter_.GetSplitOptions());
	return words_;
}

std::pair<int, int> SplitterTaskFast::Convert(unsigned int wordPosStart, unsigned int wordPosEnd) {
	if (wordPosStart < lastWordPos_) {
		lastWordPos_ = 0;
		lastOffset_ = 0;
	}

	auto ret = word2Pos(text_.substr(lastOffset_), wordPosStart - lastWordPos_, wordPosEnd - lastWordPos_, splitter_.GetSplitOptions());
	ret.first += lastOffset_;
	ret.second += lastOffset_;
	lastOffset_ = ret.first;
	lastWordPos_ = wordPosStart;
	return ret;
}

void SplitterTaskFast::WordToByteAndCharPos(int wordPosition, WordPosition& out) {
	out = wordToByteAndCharPos<WordPosition>(text_, wordPosition, splitter_.GetSplitOptions());
}

void SplitterTaskFast::WordToByteAndCharPos(int wordPosition, WordPositionEx& out) {
	out = wordToByteAndCharPos<WordPositionEx>(text_, wordPosition, splitter_.GetSplitOptions());
}

template <typename Pos>
Pos SplitterTaskFast::wordToByteAndCharPos(std::string_view str, int wordPosition, const SplitOptions& splitOptions) {
	auto wordStartIt = str.begin();
	auto wordEndIt = str.begin();
	Pos wp;
	bool constexpr needChar = std::is_same_v<Pos, WordPositionEx>;
	if constexpr (needChar) {
		wp.start.ch = -1;
	}
	for (auto it = str.begin(); it != str.end();) {
		auto ch = utf8::unchecked::next(it);
		if constexpr (needChar) {
			wp.start.ch++;
		}
		// skip not word symbols
		while (it != str.end() && !splitOptions.IsWordSymbol(ch)) {
			wordStartIt = it;
			ch = utf8::unchecked::next(it);
			if constexpr (needChar) {
				wp.start.ch++;
			}
		}
		if constexpr (needChar) {
			wp.end.ch = wp.start.ch;
		}
		while (splitOptions.IsWordSymbol(ch)) {
			wordEndIt = it;
			if constexpr (needChar) {
				wp.end.ch++;
			}
			if (it == str.end()) {
				break;
			}
			ch = utf8::unchecked::next(it);
		}

		if (wordStartIt != it) {
			if (!wordPosition) {
				break;
			} else {
				wordPosition--;
				wordStartIt = it;
			}
		}
		if constexpr (needChar) {
			wp.start.ch = wp.end.ch;
		}
	}
	if (wordPosition != 0) {
		throw Error(errParams, "wordToByteAndCharPos: incorrect input string={} wordPosition={}", str, wordPosition);
	}
	wp.SetBytePosition(wordStartIt - str.begin(), wordEndIt - str.begin());
	return wp;
}
template WordPositionEx SplitterTaskFast::wordToByteAndCharPos<WordPositionEx>(std::string_view str, int wordPosition,
																			   const SplitOptions& splitOptions);
template WordPosition SplitterTaskFast::wordToByteAndCharPos<WordPosition>(std::string_view str, int wordPosition,
																		   const SplitOptions& splitOptions);

std::shared_ptr<ISplitterTask> FastTextSplitter::CreateTask() const { return std::make_shared<SplitterTaskFast>(SplitterTaskFast(*this)); }

}  // namespace reindexer
