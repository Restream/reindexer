#pragma once

#include "itokenfilter.h"

namespace reindexer {

class [[nodiscard]] Translit : public ITokenFilter {
public:
	Translit();

	void GetVariants(const std::wstring& data, ITokenFilter::ResultsStorage& result, int proc,
					 fast_hash_map<std::wstring, size_t>& patternsUsed) override final;

private:
	void PrepareRussian();
	void PrepareEnglish();

	struct [[nodiscard]] Context {
		Context() : total_count_(0), num_{0} {}

		void Set(unsigned short num);
		unsigned short GetLast() const;
		unsigned short GetPrevious() const;

		unsigned short GetCount() const;

		void Clear();

	private:
		size_t total_count_;
		unsigned short num_[2];
	};
	std::pair<uint8_t, wchar_t> GetEnglish(wchar_t, size_t, Context& ctx);
	bool CheckIsEn(wchar_t symbol);

	static const int ruLettersStartUTF16 = 1072;
	static const int enLettersStartUTF16 = 97;
	static const int ruAlphabetSize = 32;
	static const int engAlphabetSize = 26;
	static const int maxTranslitVariants = 3;

	std::wstring ru_buf_[ruAlphabetSize][maxTranslitVariants];
	wchar_t en_buf_[engAlphabetSize];
	wchar_t en_d_buf_[engAlphabetSize][engAlphabetSize];
	wchar_t en_t_buf_[engAlphabetSize][engAlphabetSize][engAlphabetSize];
};

}  // namespace reindexer
