#pragma once

#include <map>
#include <memory>
#include "isearcher.h"

using std::map;
using std::string;

namespace search_engine {

class Translit : public ISeacher {
public:
	typedef shared_ptr<Translit> Ptr;
	Translit();

	virtual void Build(const wchar_t *data, size_t len, vector<pair<std::wstring, ProcType>> &result) override final;

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
	pair<uint8_t, wchar_t> GetEnglish(wchar_t, size_t, Context &ctx);
	bool CheckIsEn(wchar_t symbol);

	static const int ruLettersStartUTF16 = 1072;
	static const int enLettersStartUTF16 = 97;
	static const int ruAlfavitSize = 32;
	static const int enAlfavitSize = 26;
	static const int maxTraslitVariants = 3;

	wstring ru_buf_[ruAlfavitSize][maxTraslitVariants];
	wchar_t en_buf_[enAlfavitSize];
	wchar_t en_d_buf_[enAlfavitSize][enAlfavitSize];
	wchar_t en_t_buf_[enAlfavitSize][enAlfavitSize][enAlfavitSize];
};

}  // namespace search_engine
