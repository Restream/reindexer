#include "ft_base.h"
#include <fstream>
#include <ranges>
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer::bench {

Error FullTextBase::Initialize() { return readDictFile(RX_BENCH_DICT_PATH, words1_); }

std::string FullTextBase::MakeTypoWord() {
	static const std::wstring wchars =
		L"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZабвгдежзийклмнопрстуфхцчшщъыьэюяАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ";
	auto word = GetRandomUTF16WordByLength(2);
	word[randomGenerator_(randomEngine_, std::uniform_int_distribution<int>::param_type{0, int(word.length() - 1)})] =
		wchars.at(randomGenerator_(randomEngine_, std::uniform_int_distribution<int>::param_type{0, int(wchars.size() - 1)}));
	word += L"~";
	return utf16_to_utf8(word);
}

std::wstring FullTextBase::GetRandomUTF16WordByLength(size_t minLen) {
	std::wstring word;
	for (; word.length() < minLen;) {
		word = utf8_to_utf16(RndWord1());
	}
	return word;
}

std::string FullTextBase::CreatePhrase() {
	const size_t wordCnt = 100;
	WrSerializer r;
	r.Reserve(wordCnt * 30);

	for (size_t i = 0; i < wordCnt; i++) {
		r << RndWord1();
		if (i < wordCnt - 1) {
			r << " ";
		}
	}

	return std::string(r.Slice());
}

std::string FullTextBase::MakePrefixWord() {
	auto word = GetRandomUTF16WordByLength(4);

	auto pos = RndInt(2, word.length() - 2);
	word.erase(pos, word.length() - pos);
	word += L"*";

	return utf16_to_utf8(word);
}

std::string FullTextBase::MakeSuffixWord() {
	auto word = GetRandomUTF16WordByLength(4);
	auto cnt = RndInt(0, word.length() / 2);
	word.erase(0, cnt);
	word = L"*" + word;
	return utf16_to_utf8(word);
}

Error FullTextBase::readDictFile(const std::string& fileName, std::vector<std::string>& words) {
	constexpr static size_t kFileSize = 140'000;
	using InputIt = std::istream_iterator<std::string>;
	std::ifstream file;
	file.open(fileName);
	if (!file) {
		return Error(errNotValid, "{}", strerror(errno));
	}
	if (maxWord1Count_ == 0) {
		words1_.reserve(kFileSize);
		std::copy(InputIt(file), InputIt(), std::back_inserter(words));
	} else {
		words1_.reserve(std::min<size_t>(kFileSize, maxWord1Count_));
		std::ranges::copy(std::views::take(std::ranges::subrange(InputIt(file), InputIt()), maxWord1Count_), std::back_inserter(words));
	}
	return Error();
}

}  // namespace reindexer::bench
