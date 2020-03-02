#pragma once

#include "itokenfilter.h"

namespace reindexer {

class Translit : public ITokenFilter {
public:
	Translit();

	virtual void GetVariants(const std::wstring &data, std::vector<std::pair<std::wstring, int>> &result) override final;

private:
	void PrepareRussian();
	void PrepareEnglish();

	struct Context {
		Context() : total_count_(0), num_{0} {}

		void Set(unsigned short num);
		unsigned short GetLast() const;
		unsigned short GetPrevios() const;

		unsigned short GetCount() const;

		void Clear();

	private:
		size_t total_count_;
		unsigned short num_[2];
	};
	std::pair<uint8_t, wchar_t> GetEnglish(wchar_t, size_t, Context &ctx);
	bool CheckIsEn(wchar_t symbol);

	static const int ruLettersStartUTF16 = 1072;
	static const int enLettersStartUTF16 = 97;
	static const int ruAlfavitSize = 32;
	static const int enAlfavitSize = 26;
	static const int maxTraslitVariants = 3;

	std::wstring ru_buf_[ruAlfavitSize][maxTraslitVariants];
	wchar_t en_buf_[enAlfavitSize];
	wchar_t en_d_buf_[enAlfavitSize][enAlfavitSize];
	wchar_t en_t_buf_[enAlfavitSize][enAlfavitSize][enAlfavitSize];
};

}  // namespace reindexer
