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

#include "frisochartypes.h"
#include <cstring>
#include "vendor/utf8cpp/utf8/unchecked.h"

namespace reindexer {

bool FrisoCharTypes::utf8_cjk_string(uint16_t u) {
	bool c = ((u >= 0x4E00 && u <= 0x9FBF) || (u >= 0x2E80 && u <= 0x2EFF) || (u >= 0x2F00 && u <= 0x2FDF) ||
			  (u >= 0x31C0 && u <= 0x31EF)	   //|| ( u >= 0x3200 && u <= 0x32FF )
			  || (u >= 0x3300 && u <= 0x33FF)  //|| ( u >= 0x3400 && u <= 0x4DBF )
			  || (u >= 0x4DC0 && u <= 0x4DFF) || (u >= 0xF900 && u <= 0xFAFF) || (u >= 0xFE30 && u <= 0xFE4F));
	return c;
}

bool FrisoCharTypes::utf8_whitespace(uint16_t u) {
	if (u == 32 || u == 12288) {
		return true;
	}
	return false;
}

bool FrisoCharTypes::utf8_en_letter(uint16_t u) {
	if (u > 65280) {
		u -= 65248;
	}
	return ((u >= 65 && u <= 90) || (u >= 97 && u <= 122));
}

bool FrisoCharTypes::utf8_numeric_letter(uint16_t u) {
	if (u > 65280) {
		u -= 65248;	 // make full-width half-width.
	}
	return ((u >= 48 && u <= 57));
}

bool FrisoCharTypes::utf8_uppercase_letter(uint16_t u) {
	if (u > 65280) {
		u -= 65248;
	}
	return (u >= 65 && u <= 90);
}

bool FrisoCharTypes::utf8_fullwidth_en_char(uint16_t u) {
	return ((u >= 65296 && u <= 65305)		 // arabic number
			|| (u >= 65313 && u <= 65338)	 // upper case letters
			|| (u >= 65345 && u <= 65370));	 // lower case letters
}

bool FrisoCharTypes::utf8_halfwidth_en_char(uint16_t u) { return (u >= 32 && u <= 126); }

bool FrisoCharTypes::utf8_en_punctuation(uint16_t u) {
	if (u > 65280) {
		u = u - 65248;	// make full-width half-width
	}
	return ((u > 32 && u < 48) || (u > 57 && u < 65) || (u > 90 && u < 97)	// added @2013-08-31
			|| (u > 122 && u < 127));
}

bool FrisoCharTypes::utf8_cn_punctuation(uint16_t u) {
	return ((u > 65280 && u < 65296) || (u > 65305 && u < 65312) || (u > 65338 && u < 65345) ||
			(u > 65370 && u < 65382)
			// cjk symbol and punctuation.(added 2013-09-06)
			// from http://www.unicode.org/charts/PDF/U3000.pdf
			|| (u >= 12289 && u <= 12319));
}

bool FrisoCharTypes::utf8_numeric_string(std::string_view str) {
	const char* s = str.data();
	int bytes = 1;

	while (*s != '\0') {
		const char c = *s;
		if (c & (1 << 7)) {	 // full-width chars.
			const char* it = s;
			int u = utf8::unchecked::next(it);
			bytes = it - s;
			if (u < 65296 || u > 65305) {
				return false;
			}
		} else if (c < 48 || c > 57) {
			return false;
		}
		s += bytes;
	}

	return true;
}

bool FrisoCharTypes::utf8_decimal_string(std::string_view str) {
	int len = str.size(), i, p = 0;
	int bytes = 0, u;

	if (str[0] == '.' || str[len - 1] == '.') {
		return false;
	}

	for (i = 1; i < len; bytes = 1) {
		const char c = str[i];
		// count the number of char '.'
		if (c == '.') {
			i++;
			p++;
			continue;
		} else if (c & (1 << 7)) {
			// full-width numeric.
			const char* s = &str[0] + i;
			u = utf8::unchecked::next(s);
			bytes = s - (&str[0] + i);
			if (u < 65296 || u > 65305) {
				return false;
			}
		} else if (c < 48 || c > 57) {
			return false;
		}

		i += bytes;
	}

	return (p == 1);
}

bool FrisoCharTypes::friso_en_kpunc(const char* kpuncs, char ch) { return strchr(kpuncs, ch) != 0; }

friso_enchar_t FrisoCharTypes::friso_enchar_type(uint16_t u) {
	// range check.
	if (u > 126 || u < 32) {
		return FRISO_EN_UNKNOWN;
	}
	if (u == 32) {
		return FRISO_EN_WHITESPACE;
	}
	if (u >= 48 && u <= 57) {
		return FRISO_EN_NUMERIC;
	}
	if (u >= 65 && u <= 90) {
		return FRISO_EN_LETTER;
	}
	if (u >= 97 && u <= 122) {
		return FRISO_EN_LETTER;
	}

	return FRISO_EN_PUNCTUATION;
}

}  // namespace reindexer
