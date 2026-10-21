#pragma once

#include <stdint.h>
#include <string>
#include <vector>
#include "estl/h_vector.h"

namespace reindexer {

class [[nodiscard]] Translit {
public:
	Translit();
	void Transliterate(const std::u16string& data, h_vector<std::u16string, 5>& res);

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
	std::pair<uint8_t, char16_t> GetEnglish(char16_t, size_t, Context& ctx);
	bool CheckIsEn(char16_t symbol);

	static const int ruLettersStartUTF16 = 1072;
	static const int enLettersStartUTF16 = 97;
	static const int ruAlphabetSize = 32;
	static const int engAlphabetSize = 26;
	static const int maxTranslitVariants = 3;

	std::u16string ru_buf_[ruAlphabetSize][maxTranslitVariants];
	char16_t en_buf_[engAlphabetSize];
	char16_t en_d_buf_[engAlphabetSize][engAlphabetSize];
	char16_t en_t_buf_[engAlphabetSize][engAlphabetSize][engAlphabetSize];
};

}  // namespace reindexer
