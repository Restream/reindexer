#include "tools/string_regexp_functions.h"

#include "tools/customlocal.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"

namespace reindexer {

std::string makeLikePattern(std::string_view utf8Str) {
	std::wstring utf16Str = reindexer::utf8_to_utf16(utf8Str);
	for (wchar_t& ch : utf16Str) {
		if (rand() % 4 == 0) {
			ch = L'_';
		}
	}
	std::wstring result;
	if (rand() % 4 == 0) {
		result += L'%';
	}
	std::wstring::size_type next = rand() % (utf16Str.size() + 1);
	std::wstring::size_type last = next;
	for (std::wstring::size_type current = 0; current < utf16Str.size();) {
		if (current < next) {
			result += utf16Str.substr(current, next - current);
			last = next;
			current = (rand() % (utf16Str.size() - last + 1)) + last;
		}
		next = (rand() % (utf16Str.size() - current + 1)) + current;
		if (current > last || rand() % 4 == 0) {
			result += L'%';
		}
	}
	if (rand() % 4 == 0) {
		result += L'%';
	}
	return reindexer::utf16_to_utf8(result);
}

std::string sqlLikePattern2ECMAScript(std::string pattern) {
	for (std::string::size_type pos = 0; pos < pattern.size();) {
		if (pattern[pos] == '_') {
			pattern[pos] = '.';
		} else if (pattern[pos] == '%') {
			pattern.replace(pos, 1, ".*");
		}
		const char* ptr = &pattern[pos];
		utf8::unchecked::next(ptr);
		pos = ptr - pattern.data();
	}
	return pattern;
}

bool matchLikePattern(std::string_view utf8Str, std::string_view utf8Pattern) {
	constexpr static wchar_t anyChar = L'_', wildChar = L'%';
	const char* pIt = utf8Pattern.data();
	const char* const pEnd = utf8Pattern.data() + utf8Pattern.size();
	const char* sIt = utf8Str.data();
	const char* const sEnd = utf8Str.data() + utf8Str.size();
	bool haveWildChar = false;

	while (pIt != pEnd && sIt != sEnd) {
		const wchar_t pCh = utf8::unchecked::next(pIt);
		if (pCh == wildChar) {
			haveWildChar = true;
			break;
		}
		if (ToLower(pCh) != ToLower(utf8::unchecked::next(sIt)) && pCh != anyChar) {
			return false;
		}
	}

	while (pIt != pEnd && sIt != sEnd) {
		const char* tmpSIt = sIt;
		const char* tmpPIt = pIt;
		while (tmpPIt != pEnd) {
			const wchar_t pCh = utf8::unchecked::next(tmpPIt);
			if (pCh == wildChar) {
				sIt = tmpSIt;
				pIt = tmpPIt;
				haveWildChar = true;
				break;
			}
			if (tmpSIt == sEnd) {
				return false;
			}
			if (ToLower(pCh) != ToLower(utf8::unchecked::next(tmpSIt)) && pCh != anyChar) {
				utf8::unchecked::next(sIt);
				break;
			}
		}
		if (tmpPIt == pEnd) {
			sIt = tmpSIt;
			pIt = tmpPIt;
		}
	}

	while (pIt != pEnd) {
		if (utf8::unchecked::next(pIt) != wildChar) {
			return false;
		}
		haveWildChar = true;
	}

	if (!haveWildChar && sIt != sEnd) {
		return false;
	}

	for (pIt = pEnd, sIt = sEnd; pIt != utf8Pattern.data() && sIt != utf8Str.data();) {
		const wchar_t pCh = utf8::unchecked::prior(pIt);
		if (pCh == wildChar) {
			return true;
		}
		if (ToLower(pCh) != ToLower(utf8::unchecked::prior(sIt)) && pCh != anyChar) {
			return false;
		}
	}
	return true;
}

}  // namespace reindexer
