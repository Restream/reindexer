#include "tools/string_regexp_functions.h"

#include "tools/customlocal.h"
#include "utf8cpp/utf8.h"

namespace reindexer {

bool matchLikePattern(std::string_view utf8Str, std::string_view utf8Pattern) {
	constexpr static uint32_t anyChar = u'_', wildChar = u'%';
	const char* pIt = utf8Pattern.data();
	const char* const pEnd = utf8Pattern.data() + utf8Pattern.size();
	const char* sIt = utf8Str.data();
	const char* const sEnd = utf8Str.data() + utf8Str.size();
	bool haveWildChar = false;

	while (pIt != pEnd && sIt != sEnd) {
		const uint32_t pCh = utf8::unchecked::next(pIt);
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
			const uint32_t pCh = utf8::unchecked::next(tmpPIt);
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
		const uint32_t pCh = utf8::unchecked::prior(pIt);
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
