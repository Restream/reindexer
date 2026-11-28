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
#include <cstdint>
#include <string_view>

namespace reindexer {

enum [[nodiscard]] friso_enchar_t {
	FRISO_EN_LETTER = 0,	   // A-Z, a-z
	FRISO_EN_NUMERIC = 1,	   // 0-9
	FRISO_EN_PUNCTUATION = 2,  // english punctuations
	FRISO_EN_WHITESPACE = 3,   // whitespace
	FRISO_EN_UNKNOWN = -1	   // unkown(beyond 32-126)
};

namespace FrisoCharTypes {
bool utf8_cjk_string(uint16_t u);
bool utf8_whitespace(uint16_t u);
bool utf8_en_letter(uint16_t u);
bool utf8_numeric_letter(uint16_t u);
bool utf8_uppercase_letter(uint16_t u);
bool utf8_fullwidth_en_char(uint16_t u);
bool utf8_halfwidth_en_char(uint16_t u);
bool utf8_en_punctuation(uint16_t u);
bool utf8_cn_punctuation(uint16_t u);
friso_enchar_t friso_enchar_type(uint16_t u);
bool utf8_numeric_string(std::string_view str);
bool utf8_decimal_string(std::string_view str);
bool friso_en_kpunc(const char* kpuncs, char ch);
};	// namespace FrisoCharTypes

}  // namespace reindexer
