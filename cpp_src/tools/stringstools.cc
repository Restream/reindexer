#include "tools/stringstools.h"
#include "atoi/atoi.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/uuid.h"
#include "fmt/compile.h"
#include "frozen_str_tools.h"
#include "itoa/itoa.h"
#include "tools/assertrx.h"
#include "tools/randomgenerator.h"
#include "tools/serializer.h"
#include "utf8cpp/utf8.h"
#include "vendor/double-conversion/double-conversion.h"
#include "vendor/frozen/unordered_map.h"

namespace reindexer {

const char* kDefaultExtraWordsSymbols = "-/+_`'";
const char* kDefaultWordPartDelimiters = "-/+_`'";

namespace stringtools_impl {

static std::string_view urldecode2(char* buf, std::string_view str) {
	char a, b;
	const char* src = str.data();
	char* dst = buf;

	for (size_t l = 0; l < str.length(); l++) {
		if (l + 2 < str.length() && (*src == '%') && ((a = src[1]) && (b = src[2])) && (isxdigit(a) && isxdigit(b))) {
			if (a >= 'a') {
				a -= 'a' - 'A';
			}
			if (a >= 'A') {
				a -= ('A' - 10);
			} else {
				a -= '0';
			}
			if (b >= 'a') {
				b -= 'a' - 'A';
			}
			if (b >= 'A') {
				b -= ('A' - 10);
			} else {
				b -= '0';
			}
			*dst++ = 16 * a + b;
			src += 3;
			l += 2;
		} else if (*src == '+') {
			*dst++ = ' ';
			src++;
		} else {
			*dst++ = *src++;
		}
	}
	*dst = '\0';
	return std::string_view(buf, dst - buf);
}

static bool isHexDigit(char c) { return (c >= '0' && c <= '9') || ((c & ~' ') >= 'A' && (c & ~' ') <= 'F'); }

static int charToInt(char c) {
	if (c <= '9') {
		return c - '0';
	}
	return (c & ~' ') - 'A' + 10;
}

// Sat Jul 15 14 : 18 : 56 2017 GMT

static const char* kDaysOfWeek[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
static const char* kMonths[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

inline static char* strappend(char* dst, const char* src) noexcept {
	while (*src) {
		*dst++ = *src++;
	}
	return dst;
}

constexpr static auto kLogLevels = frozen::make_unordered_map<std::string_view, LogLevel>(
	{{"none", LogNone}, {"warning", LogWarning}, {"error", LogError}, {"info", LogInfo}, {"trace", LogTrace}}, frozen::nocase_hash_str{},
	frozen::nocase_equal_str{});

constexpr static auto kStrictModes = frozen::make_unordered_map<std::string_view, StrictMode>(
	{{"", StrictModeNotSet}, {"none", StrictModeNone}, {"names", StrictModeNames}, {"indexes", StrictModeIndexes}},
	frozen::nocase_hash_str{}, frozen::nocase_equal_str{});

}  // namespace stringtools_impl

bool SplitOptions::IsWord(std::string_view str) const noexcept {
	for (auto it = str.begin(), endIt = str.end(); it != endIt;) {
		if (!IsWordSymbol(utf8::unchecked::next(it))) {
			return false;
		}
	}

	return true;
}

void SplitOptions::RemoveDelims(std::string_view str, std::string& res) const {
	res.resize(str.length());
	auto resBegin = res.begin();
	auto resIt = res.begin();

	for (auto it = str.begin(), endIt = str.end(); it != endIt;) {
		uint32_t ch = utf8::unchecked::next(it);
		if (!IsWordPartDelimiter(ch)) {
			resIt = utf8::unchecked::append(ch, resIt);
		}
	}

	res.resize(std::distance(resBegin, resIt));
}

std::string SplitOptions::RemoveDelims(std::string_view str) const {
	std::string buf;
	RemoveDelims(str, buf);
	return buf;
}

bool SplitOptions::ContainsDelims(std::string_view str) const {
	for (auto it = str.begin(), endIt = str.end(); it != endIt;) {
		if (IsWordPartDelimiter(utf8::unchecked::next(it))) {
			return true;
		}
	}

	return false;
}

bool SplitOptions::ContainsDelims(std::wstring_view str) const {
	return std::ranges::any_of(str, [this](auto ch) { return IsWordPartDelimiter(ch); });
}

std::string SplitOptions::RemoveAccentsAndDiacritics(std::string_view str) const {
	if (!removeDiacriticsMask_) {
		return std::string(str);
	}

	std::string buf;
	buf.resize(str.length());
	auto bufBegin = buf.begin();
	auto bufIt = buf.begin();

	for (auto it = str.begin(), endIt = str.end(); it != endIt;) {
		uint32_t ch = utf8::unchecked::next(it);
		if (NeedToRemoveDiacritics(ch)) {
			ch = RemoveDiacritic(ch);
		}
		if (ch != 0) {
			bufIt = utf8::unchecked::append(ch, bufIt);
		}
	}

	buf.resize(std::distance(bufBegin, bufIt));
	return buf;
}

static void setMask(std::vector<bool>& mask, uint32_t ch) noexcept {
	if (ch < UINT16_MAX) {
		if (mask.size() < ch + 1) {
			mask.resize(ch + 1);
		}
		mask[ch] = true;
	}
}

void SplitOptions::SetSymbols(std::string_view extraWordSymbols, std::string_view wordPartDelimiters) {
	extraWordSymbolsMask_.resize(0);
	wordPartDelimitersMask_.resize(0);
	extraWordSymbolsMask_.reserve(128);
	wordPartDelimitersMask_.reserve(128);

	hasDelims_ = !wordPartDelimiters.empty();

	if (!extraWordSymbols.empty()) {
		for (auto it = extraWordSymbols.begin(); it != extraWordSymbols.end();) {
			auto ch = utf8::unchecked::next(it);
			setMask(extraWordSymbolsMask_, ch);
		}
	}

	if (!wordPartDelimiters.empty()) {
		for (auto it = wordPartDelimiters.begin(); it != wordPartDelimiters.end();) {
			auto ch = utf8::unchecked::next(it);
			setMask(wordPartDelimitersMask_, ch);
			setMask(extraWordSymbolsMask_, ch);
		}
	}
}

static std::string bitmaskAsString(const std::vector<bool>& mask) {
	std::string res;
	std::wstring str;
	for (size_t i = 0; i < mask.size(); ++i) {
		if (mask[i]) {
			str.push_back(wchar_t(i));
		}
	}
	utf16_to_utf8(str, res);
	return res;
}

std::string SplitOptions::GetExtraWordSymbols() const { return bitmaskAsString(extraWordSymbolsMask_); }
std::string SplitOptions::GetWordPartDelimiters() const { return bitmaskAsString(wordPartDelimitersMask_); }

std::string toLower(std::string_view src) {
	std::string ret;
	ret.reserve(src.size());
	for (char ch : src) {
		ret.push_back(tolower(ch));
	}
	return ret;
}

std::string escapeString(std::string_view str) {
	std::string dst;
	dst.reserve(str.length());
	std::ignore = escapeString(str.begin(), str.end(), std::back_inserter(dst), AddQuotes_False);
	return dst;
}

std::string unescapeString(std::string_view str) {
	std::string dst;
	dst.reserve(str.length());

	for (size_t i = 0; i < str.length(); ++i) {
		if (str[i] == '\\' && i + 1 < str.length()) {
			++i;
			switch (str[i]) {
				case '"':
					dst.push_back('"');
					break;
				case '\\':
					dst.push_back('\\');
					break;
				case '/':
					dst.push_back('/');
					break;
				case 'b':
					dst.push_back('\b');
					break;
				case 'f':
					dst.push_back('\f');
					break;
				case 'n':
					dst.push_back('\n');
					break;
				case 'r':
					dst.push_back('\r');
					break;
				case 't':
					dst.push_back('\t');
					break;
				case 'u':
					if (i + 4 < str.length()) {
						bool is_valid_hex = true;
						unsigned int code = 0;
						size_t curr_pos = i;

						for (int j = 0; j < 4; ++j) {
							char digit = str[++curr_pos];
							if (stringtools_impl::isHexDigit(digit)) {
								code = code * 16 + stringtools_impl::charToInt(digit);
							} else {
								is_valid_hex = false;
								break;
							}
						}

						if (is_valid_hex && code > 0 && code <= 0xFFFF) {
							if (code < 0x80) {
								dst += static_cast<char>(code);
							} else if (code < 0x800) {
								dst += static_cast<char>(0xC0 | (code >> 6));
								dst += static_cast<char>(0x80 | (code & 0x3F));
							} else {
								dst += static_cast<char>(0xE0 | (code >> 12));
								dst += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
								dst += static_cast<char>(0x80 | (code & 0x3F));
							}
							i = curr_pos;
						} else {
							dst += "\\u";
						}
					} else {
						dst += "\\u";
					}
					break;
				default:
					dst.push_back('\\');
					dst.push_back(str[i]);
					break;
			}
		} else {
			dst.push_back(str[i]);
		}
	}
	return dst;
}

KeyValueType detectValueType(std::string_view value) {
	if (value.empty()) {
		return KeyValueType::Undefined{};
	}
	static std::string_view trueToken = "true", falseToken = "false";
	size_t i = 0;
	bool isDouble = false, isDigit = false, isBool = false;
	if (isdigit(value[i])) {
		isDigit = true;
	} else if (value.size() > 1) {
		if (value[i] == '-' || value[i] == '+') {
			isDigit = value[i + 1] != '.';
		} else {
			isBool = (value[i] == trueToken[i] || value[i] == falseToken[i]);
		}
	} else {
		return KeyValueType::String{};
	}
	for (++i; i < value.length() && (isDouble || isDigit || isBool); i++) {
		if (isDigit || isDouble) {
			if (!isdigit(value[i])) {
				if (value[i] == '.') {
					if (isDouble) {
						return KeyValueType::String{};
					}
					isDouble = true;
				} else {
					return KeyValueType::String{};
				}
			}
		} else if (isBool) {
			isBool &= (i < trueToken.size() && trueToken[i] == value[i]) || (i < falseToken.size() && falseToken[i] == value[i]);
		}
	}
	if (isDigit) {
		if (isDouble) {
			return KeyValueType::Double{};
		}
		return KeyValueType::Int64{};
	} else if (isBool) {
		return KeyValueType::Bool{};
	} else {
		return KeyValueType::String{};
	}
}

Variant stringToVariant(std::string_view value) {
	const auto kvt = detectValueType(value);
	return kvt.EvaluateOneOf(
		[value](KeyValueType::Int64) { return Variant(int64_t(stoll(value))); },
		[value](KeyValueType::Int) { return Variant(int(stoi(value))); },
		[value](KeyValueType::Double) {
			using double_conversion::StringToDoubleConverter;
			static const StringToDoubleConverter converter{StringToDoubleConverter::NO_FLAGS, NAN, NAN, nullptr, nullptr};
			int countOfCharsParsedAsDouble = 0;
			return Variant(converter.StringToDouble(value.data(), value.size(), &countOfCharsParsedAsDouble));
		},
		[value](KeyValueType::Float) {
			using double_conversion::StringToDoubleConverter;
			static const StringToDoubleConverter converter{StringToDoubleConverter::NO_FLAGS, NAN, NAN, nullptr, nullptr};
			int countOfCharsParsedAsDouble = 0;
			return Variant(converter.StringToFloat(value.data(), value.size(), &countOfCharsParsedAsDouble));
		},
		[value](KeyValueType::String) { return Variant(make_key_string(value.data(), value.length())); },
		[value](KeyValueType::Bool) noexcept { return (value.size() == 4) ? Variant(true) : Variant(false); },
		[value](KeyValueType::Uuid) { return Variant{Uuid{value}}; },
		[](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
						   KeyValueType::FloatVector> auto) noexcept { return Variant(); });
}

void utf8_to_utf16(std::string_view src, std::wstring& dst) {
	dst.resize(src.length());
	auto end = utf8::unchecked::utf8to32(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
}

void utf16_to_utf8(const std::wstring_view src, std::string& dst) {
	dst.resize(src.length() * 4);
	auto end = utf8::unchecked::utf32to8(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
}

std::wstring utf8_to_utf16(std::string_view src) {
	std::wstring dst;
	utf8_to_utf16(src, dst);
	return dst;
}
std::string utf16_to_utf8(const std::wstring_view src) {
	std::string dst;
	utf16_to_utf8(src, dst);
	return dst;
}
size_t utf16_to_utf8_size(const std::wstring_view src) { return utf8::unchecked::utf32to8_size(src.begin(), src.end()); }

// This functions calculate how many bytes takes limit symbols in UTF8 forward
size_t calcUtf8After(std::string_view str, size_t limit) noexcept {
	const char* ptr;
	const char* strEnd = str.data() + str.size();
	for (ptr = str.data(); limit && ptr < strEnd; limit--) {
		utf8::unchecked::next(ptr);
	}
	return ptr - str.data();
}
std::pair<size_t, size_t> calcUtf8AfterDelims(std::string_view str, size_t limit, std::string_view delims) noexcept {
	const char* ptr;
	const char* strEnd;
	const char* ptrDelims;
	const char* delimsEnd;
	size_t charCounter = 0;
	for (ptr = str.data(), strEnd = str.data() + str.size(); limit && ptr < strEnd; limit--) {
		uint32_t c = utf8::unchecked::next(ptr);
		if (IsDiacritic(c)) {
			++limit;
			continue;
		}

		++charCounter;
		for (ptrDelims = delims.data(), delimsEnd = delims.data() + delims.size(); ptrDelims < delimsEnd;) {
			uint32_t d = utf8::unchecked::next(ptrDelims);
			if (c == d) {
				utf8::unchecked::prior(ptr);
				return std::make_pair(ptr - str.data(), charCounter - 1);
			}
		}
	}

	// Add all diacritics after last symbol
	while (ptr < strEnd) {
		uint32_t c = utf8::unchecked::next(ptr);
		if (!IsDiacritic(c)) {
			utf8::unchecked::prior(ptr);
			break;
		}
	}

	return std::make_pair(ptr - str.data(), charCounter);
}

// This functions calculate how many bytes takes limit symbols in UTF8 backward
size_t calcUtf8Before(const char* str, int pos, size_t limit) noexcept {
	const char* ptr = str + pos;
	for (; limit && ptr > str; limit--) {
		utf8::unchecked::prior(ptr);
	}
	return str + pos - ptr;
}

std::pair<size_t, size_t> calcUtf8BeforeDelims(const char* str, int pos, size_t limit, std::string_view delims) noexcept {
	const char* ptr = str + pos;
	const char* ptrDelim;
	const char* delimsEnd;
	int charCounter = 0;
	for (; limit && ptr > str; limit--) {
		uint32_t c = utf8::unchecked::prior(ptr);
		if (IsDiacritic(c)) {
			++limit;
			continue;
		}
		++charCounter;
		for (ptrDelim = delims.data(), delimsEnd = delims.data() + delims.size(); ptrDelim < delimsEnd;) {
			uint32_t d = utf8::unchecked::next(ptrDelim);
			if (c == d) {
				utf8::unchecked::next(ptr);
				return std::make_pair(str + pos - ptr, charCounter - 1);
			}
		}
	}
	return std::make_pair(str + pos - ptr, charCounter);
}

void split(std::string_view str, std::string& buf, std::vector<WordWithPos>& words, const SplitOptions& options) {
	// assuming that the 'ToLower' function and the 'check for replacement' function should not change the character size in bytes
	// allocating additional memory to avoid resizing
	buf.resize(2 * str.length());
	std::string::iterator bufIt = buf.begin();
	words.resize(0);
	unsigned wordPos = 0;

	for (auto it = str.begin(), endIt = str.end(); it != endIt;) {
		auto ch = utf8::unchecked::next(it);

		// word can't begin from quote even if quotes present in extra_word_symbols
		while (!options.IsWordSymbol(ch) && it != endIt) {
			ch = utf8::unchecked::next(it);
		}

		auto wordBeginIt = bufIt;
		auto wordPartBeginIt = bufIt;
		size_t wordPartLength = 0;
		size_t numPartsFound = 0;

		while (options.IsWordSymbol(ch)) {
			ch = ToLower(ch);
			if (options.NeedToRemoveDiacritics(ch)) {
				ch = RemoveDiacritic(ch);
			}
			const bool isDelimiter = options.IsWordPartDelimiter(ch);

			// add all delimited parts
			if (isDelimiter) {
				if (wordPartBeginIt != bufIt) {
					numPartsFound++;
					if (wordPartLength >= options.GetMinPartSize()) {
						words.emplace_back(std::string_view(&(*wordPartBeginIt), std::distance(wordPartBeginIt, bufIt)), wordPos);
					}
				}
			}

			if (ch != 0) {
				bufIt = utf8::unchecked::append(ch, bufIt);
				++wordPartLength;
			}

			if (isDelimiter) {
				wordPartBeginIt = bufIt;
				wordPartLength = 0;
			}

			if (it != endIt) {
				ch = utf8::unchecked::next(it);
			} else {
				break;
			}
		}

		// add last part
		if (wordPartBeginIt != bufIt) {
			numPartsFound++;
			if (wordPartBeginIt != wordBeginIt && wordPartLength >= options.GetMinPartSize()) {
				words.emplace_back(std::string_view(&(*wordPartBeginIt), std::distance(wordPartBeginIt, bufIt)), wordPos);
			}
		}

		// add word
		const size_t wordLen = std::distance(wordBeginIt, bufIt);
		if (wordBeginIt != bufIt) {
			words.emplace_back(std::string_view(&(*wordBeginIt), wordLen), wordPos);
		}

		// add word without delims
		if (numPartsFound > 1) {
			const auto wordEndIt = bufIt;

			for (auto wordIt = wordBeginIt; wordIt != wordEndIt;) {
				const uint32_t wordCh = utf8::unchecked::next(wordIt);
				if (!options.IsWordPartDelimiter(wordCh)) {
					bufIt = utf8::unchecked::append(wordCh, bufIt);
				}
			}

			if (bufIt != wordEndIt) {
				size_t newWordLen = std::distance(wordEndIt, bufIt);
				words.emplace_back(std::string_view(&(*wordEndIt), newWordLen), wordPos);
			}
		}

		++wordPos;
	}

	buf.resize(std::distance(buf.begin(), bufIt));
}

void split(std::string_view utf8Str, std::wstring& utf16str, std::vector<std::wstring>& words, const SplitOptions& options) {
	utf8_to_utf16(utf8Str, utf16str);
	words.resize(0);
	for (auto it = utf16str.begin(); it != utf16str.end();) {
		while (it != utf16str.end() && !IsAlpha(*it) && !IsDigit(*it)) {
			++it;
		}

		auto begIt = it;
		while (it != utf16str.end() && options.IsWordSymbol(*it)) {
			*it = ToLower(*it);
			++it;
		}
		size_t sz = it - begIt;

		if (sz) {
			words.emplace_back(&*begIt, &*(begIt + sz));
		}
	}
}

template <CaseSensitive sensitivity>
bool checkIfStartsWith(std::string_view pattern, std::string_view str) noexcept {
	if (pattern.empty()) {
		return false;
	}
	if (pattern.length() > str.length()) {
		return false;
	}
	if constexpr (sensitivity == CaseSensitive::Yes) {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (pattern[i] != str[i]) {
				return false;
			}
		}
	} else {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (tolower(pattern[i]) != tolower(str[i])) {
				return false;
			}
		}
	}
	return true;
}
template bool checkIfStartsWith<CaseSensitive::Yes>(std::string_view pattern, std::string_view src) noexcept;
template bool checkIfStartsWith<CaseSensitive::No>(std::string_view pattern, std::string_view src) noexcept;

template <CaseSensitive sensitivity>
bool checkIfEndsWith(std::string_view pattern, std::string_view src) noexcept {
	if (pattern.length() > src.length()) {
		return false;
	}
	if (pattern.length() == 0) {
		return true;
	}
	const auto offset = src.length() - pattern.length();
	if constexpr (sensitivity == CaseSensitive::Yes) {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (src[offset + i] != pattern[i]) {
				return false;
			}
		}
	} else {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (tolower(src[offset + i]) != tolower(pattern[i])) {
				return false;
			}
		}
	}
	return true;
}
template bool checkIfEndsWith<CaseSensitive::Yes>(std::string_view pattern, std::string_view src) noexcept;
template bool checkIfEndsWith<CaseSensitive::No>(std::string_view pattern, std::string_view src) noexcept;

template <>
ComparationResult collateCompare<CollateASCII>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept {
	auto itl = lhs.begin();
	auto itr = rhs.begin();

	for (; itl != lhs.end() && itr != rhs.end();) {
		auto chl = tolower(*itl++);
		auto chr = tolower(*itr++);

		if (chl > chr) {
			return ComparationResult::Gt;
		}
		if (chl < chr) {
			return ComparationResult::Lt;
		}
	}

	if (lhs.size() > rhs.size()) {
		return ComparationResult::Gt;
	} else if (lhs.size() < rhs.size()) {
		return ComparationResult::Lt;
	}

	return ComparationResult::Eq;
}

template <>
ComparationResult collateCompare<CollateUTF8>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept {
	auto itl = lhs.data();
	auto itr = rhs.data();

	for (auto lhsEnd = lhs.data() + lhs.size(), rhsEnd = rhs.size() + rhs.data(); itl != lhsEnd && itr != rhsEnd;) {
		auto chl = ToLower(utf8::unchecked::next(itl));
		auto chr = ToLower(utf8::unchecked::next(itr));

		if (chl > chr) {
			return ComparationResult::Gt;
		}
		if (chl < chr) {
			return ComparationResult::Lt;
		}
	}

	if (lhs.size() > rhs.size()) {
		return ComparationResult::Gt;
	} else if (lhs.size() < rhs.size()) {
		return ComparationResult::Lt;
	}
	return ComparationResult::Eq;
}

static long int strntol(std::string_view str, const char** end, int base) noexcept {
	char buf[24];
	long int ret;
	const char* beg = str.data();
	auto sz = str.size();
	for (; beg && sz && *beg == ' '; beg++, sz--);
	assertrx_dbg(end);

	if (!sz || sz >= sizeof(buf)) {
		*end = str.data();
		return 0;
	}

	// beg can not be null if sz != 0
	// NOLINTNEXTLINE(clang-analyzer-core.NonNullParamChecker)
	std::memcpy(buf, beg, sz);
	buf[sz] = '\0';
	ret = std::strtol(buf, const_cast<char**>(end), base);
	if (ret == LONG_MIN || ret == LONG_MAX) {
		return ret;
	}
	*end = str.data() + (*end - buf);
	return ret;
}

template <>
ComparationResult collateCompare<CollateNumeric>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept {
	const char* posl = nullptr;
	const char* posr = nullptr;

	int numl = strntol(lhs, &posl, 10);
	int numr = strntol(rhs, &posr, 10);

	if (numl == numr) {
		auto minlen = std::min(lhs.size() - (posl - lhs.data()), rhs.size() - (posr - rhs.data()));
		auto res = strncmp(posl, posr, minlen);

		if (res != 0) {
			return res < 0 ? ComparationResult::Lt : ComparationResult::Gt;
		}

		return lhs.size() > rhs.size() ? ComparationResult::Gt : (lhs.size() < rhs.size() ? ComparationResult::Lt : ComparationResult::Eq);
	}

	return numl > numr ? ComparationResult::Gt : (numl < numr ? ComparationResult::Lt : ComparationResult::Eq);
}

template <>
ComparationResult collateCompare<CollateCustom>(std::string_view lhs, std::string_view rhs,
												const SortingPrioritiesTable& sortOrderTable) noexcept {
	auto itl = lhs.data();
	auto itr = rhs.data();

	for (; itl != lhs.data() + lhs.size() && itr != rhs.size() + rhs.data();) {
		auto chl = utf8::unchecked::next(itl);
		auto chr = utf8::unchecked::next(itr);

		int chlPriority = sortOrderTable.GetPriority(chl);
		int chrPriority = sortOrderTable.GetPriority(chr);

		if (chlPriority > chrPriority) {
			return ComparationResult::Gt;
		}
		if (chlPriority < chrPriority) {
			return ComparationResult::Lt;
		}
	}

	if (lhs.size() > rhs.size()) {
		return ComparationResult::Gt;
	} else if (lhs.size() < rhs.size()) {
		return ComparationResult::Lt;
	}
	return ComparationResult::Eq;
}

template <>
ComparationResult collateCompare<CollateNone>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept {
	size_t l1 = lhs.size();
	size_t l2 = rhs.size();
	int res = memcmp(lhs.data(), rhs.data(), std::min(l1, l2));

	return res ? (res < 0 ? ComparationResult::Lt : ComparationResult::Gt)
			   : ((l1 < l2)	  ? ComparationResult::Lt
				  : (l1 > l2) ? ComparationResult::Gt
							  : ComparationResult::Eq);
}

std::string urldecode2(std::string_view str) {
	std::string ret(str.length(), ' ');
	std::string_view sret = stringtools_impl::urldecode2(&ret[0], str);
	ret.resize(sret.size());
	return ret;
}

int fast_strftime(char* buf, const tm* tm) {
	char* d = buf;

	if (unsigned(tm->tm_wday) < sizeof(stringtools_impl::kDaysOfWeek) / sizeof stringtools_impl::kDaysOfWeek[0]) {
		d = stringtools_impl::strappend(d, stringtools_impl::kDaysOfWeek[tm->tm_wday]);
	}
	d = stringtools_impl::strappend(d, ", ");
	d = i32toa(tm->tm_mday, d);
	*d++ = ' ';
	if (unsigned(tm->tm_mon) < sizeof(stringtools_impl::kMonths) / sizeof stringtools_impl::kMonths[0]) {
		d = stringtools_impl::strappend(d, stringtools_impl::kMonths[tm->tm_mon]);
	}
	*d++ = ' ';
	d = i32toa(tm->tm_year + 1900, d);
	*d++ = ' ';
	d = i32toa(tm->tm_hour, d);
	*d++ = ':';
	d = i32toa(tm->tm_min, d);
	*d++ = ':';
	d = i32toa(tm->tm_sec, d);
	d = stringtools_impl::strappend(d, " GMT");
	*d = 0;
	return d - buf;
}

bool validateObjectName(std::string_view name, bool allowSpecialChars) noexcept {
	if (!name.length()) {
		return false;
	}
	return std::all_of(name.cbegin(), name.cend(), [allowSpecialChars](auto ch) {
		return (std::isalnum(ch) || ch == '_' || ch == '-' || ch == '#' || (ch == '@' && allowSpecialChars));
	});
}

bool validateUserNsName(std::string_view name) noexcept {
	if (!name.length()) {
		return false;
	}
	return std::all_of(name.cbegin(), name.cend(), [](auto ch) { return (std::isalnum(ch) || ch == '_' || ch == '-'); });
}

LogLevel logLevelFromString(std::string_view strLogLevel) noexcept {
	auto configLevelIt = stringtools_impl::kLogLevels.find(strLogLevel);
	if (configLevelIt != stringtools_impl::kLogLevels.end()) {
		return configLevelIt->second;
	}
	return LogNone;
}

std::string_view logLevelToString(LogLevel level) noexcept {
	for (auto& it : stringtools_impl::kLogLevels) {
		if (it.second == level) {
			return it.first;
		}
	}
	constexpr static std::string_view kNone("none");
	return kNone;
}

StrictMode strictModeFromString(std::string_view strStrictMode) {
	auto configModeIt = stringtools_impl::kStrictModes.find(strStrictMode);
	if (configModeIt != stringtools_impl::kStrictModes.end()) {
		return configModeIt->second;
	}
	return StrictModeNotSet;
}

std::string_view strictModeToString(StrictMode mode) {
	for (auto& it : stringtools_impl::kStrictModes) {
		if (it.second == mode) {
			return it.first;
		}
	}
	return std::string_view();
}

bool isPrintable(std::string_view str) noexcept {
	if (str.length() > 256) {
		return false;
	}
	return std::all_of(str.cbegin(), str.cend(), [](auto ch) { return (ch >= 0x20); });
}

bool isBlank(std::string_view str) noexcept {
	if (str.empty()) {
		return true;
	}
	return std::all_of(str.cbegin(), str.cend(), [](auto ch) { return isspace(ch); });
}

bool endsWith(const std::string& source, std::string_view ending) noexcept {
	if (source.length() >= ending.length()) {
		return (0 == source.compare(source.length() - ending.length(), ending.length(), ending));
	} else {
		return false;
	}
}

std::string& ensureEndsWith(std::string& source, std::string_view ending) {
	if (!source.empty() && !endsWith(source, ending)) {
		source += ending;
	}
	return source;
}

int getUTF8StringCharactersCount(std::string_view str) noexcept {
	int len = 0;
	try {
		for (auto it = str.begin(); it != str.end(); ++len) {
			utf8::next(it, str.end());
		}
	} catch (const std::exception&) {
		return str.length();
	}
	return len;
}

int stoi(std::string_view sl) {
	bool valid;
	const int res = jsteemann::atoi<int>(sl.data(), sl.data() + sl.size(), valid);
	if (!valid) {
		throw Error(errParams, "Can't convert '{}' to number", sl);
	}
	return res;
}

std::optional<int> try_stoi(std::string_view sl) {
	bool valid;
	const int res = jsteemann::atoi<int>(sl.data(), sl.data() + sl.size(), valid);
	if (!valid) {
		return std::nullopt;
	}
	return res;
}

int64_t stoll(std::string_view sl) {
	bool valid;
	auto ret = jsteemann::atoi<int64_t>(sl.data(), sl.data() + sl.size(), valid);
	if (!valid) {
		throw Error(errParams, "Can't convert '{}' to number", sl);
	}
	return ret;
}

namespace {
template <typename FPT>
int fp_spec_to_str_impl(FPT v, char* buf, int capacity) {
	if (!std::isnan(v) && !std::isinf(v)) {
		return 0;
	}
	static const std::string strNull = "null";
	const auto sz = strNull.size();
	(void)capacity;
	assertrx_dbg(sz < size_t(capacity));
	memcpy(buf, strNull.c_str(), sizeof(std::string::value_type) * sz);
	return sz;
}
}  // namespace

template <typename FPT>
int fp_to_str_impl(FPT v, char* buf, int capacity) {
	auto sz = fp_spec_to_str_impl(v, buf, capacity);
	if (sz > 0) {
		return sz;
	}

	auto end = fmt::format_to(buf, FMT_COMPILE("{}"), v);
	auto p = buf;
	do {
		if (*p == '.' || *p == 'e') {
			break;
		}
	} while (++p != end);

	if (p == end) {
		*end++ = '.';
		*end++ = '0';
	}

	assertrx_dbg(end - buf < capacity);
	return end - buf;
}

template <typename FPT>
int fp_to_str_no_trailing_impl(FPT v, char* buf, int capacity) {
	auto sz = fp_spec_to_str_impl(v, buf, capacity);
	if (sz > 0) {
		return sz;
	}

	auto end = fmt::format_to(buf, FMT_COMPILE("{}"), v);
	assertrx_dbg(end - buf < capacity);
	return end - buf;
}

template <typename FPT>
std::string fp_to_str_impl(FPT v) {
	std::string res;
	res.resize(32);
	auto len = fp_to_str_impl(v, res.data(), res.size());
	res.resize(len);
	return res;
}

int double_to_str(double v, char* buf, int capacity) { return fp_to_str_impl(v, buf, capacity); }
int double_to_str_no_trailing(double v, char* buf, int capacity) { return fp_to_str_no_trailing_impl(v, buf, capacity); }
std::string double_to_str(double v) { return fp_to_str_impl(v); }

int float_to_str(float v, char* buf, int capacity) { return fp_to_str_impl(v, buf, capacity); }
int float_to_str_no_trailing(float v, char* buf, int capacity) { return fp_to_str_no_trailing_impl(v, buf, capacity); }
std::string float_to_str(float v) { return fp_to_str_impl(v); }

void float_vector_to_str(ConstFloatVectorView view, WrSerializer& ser) {
	ser << '[';
	if (view.IsStripped()) {
		throw Error(errLogic, "Unable to serialize stripped float_vector");
	}
	auto span = view.Span();
	std::string res;
	res.resize(32);
	for (auto it = span.begin(), end = span.end(); it != end; ++it) {
		auto len = float_to_str(*it, res.data(), res.size());
		ser << std::string_view(res.data(), len);
		if (it + 1 != end) {
			ser << ',';
		}
	}
	ser << ']';
}

std::string float_vector_to_str(ConstFloatVectorView view) {
	WrSerializer ser;
	float_vector_to_str(std::move(view), ser);
	return std::string(ser.Slice());
}

std::string randStringAlph(size_t len) {
	constexpr std::string_view symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	std::string result;
	result.reserve(len);
	while (result.size() < len) {
		const size_t f = tools::RandomGenerator::getu32(0, symbols.size() - 1);
		result += symbols[f];
	}
	return result;
}

template <bool isUtf8>
void toNextCh(std::string_view::iterator& it, std::string_view str) {
	if (isUtf8) {
		utf8::next(it, str.end());
	} else {
		++it;
	}
}

template <bool isUtf8>
void toPrevCh(std::string_view::iterator& it, std::string_view str) {
	if (isUtf8) {
		utf8::previous(it, str.begin());
	} else {
		--it;
	}
}

template <bool isUtf8>
Error getBytePosInMultilineString(std::string_view str, const size_t line, const size_t charPos, size_t& bytePos) {
	auto it = str.begin();
	size_t currLine = 0, currCharPos = 0;
	for (size_t i = 0; it != str.end() && ((currLine != line) || (currCharPos != charPos)); toNextCh<isUtf8>(it, str), ++i) {
		if (*it == '\n') {
			++currLine;
		} else if (currLine == line) {
			++currCharPos;
		}
	}
	if ((currLine == line) && (charPos == currCharPos)) {
		bytePos = it - str.begin() - 1;
		return Error();
	}
	return Error(errNotValid, "Wrong cursor position: line={}, pos={}", line, charPos);
}

Error cursorPosToBytePos(std::string_view str, size_t line, size_t charPos, size_t& bytePos) {
	try {
		return getBytePosInMultilineString<true>(str, line, charPos, bytePos);
	} catch (const utf8::exception&) {
		return getBytePosInMultilineString<false>(str, line, charPos, bytePos);
	}
}

void charMultilinePos(std::string_view str, size_t pos, size_t search_start, size_t& line, size_t& col) noexcept {
	for (auto it = str.begin() + search_start; it != str.begin() + pos; it++) {
		if (*it == '\n') {
			line++;
			col = 0;
		} else {
			col++;
		}
	}
}

std::string_view trimSpaces(std::string_view str) noexcept {
	size_t left = 0, right = str.size();
	while (left < right && std::isspace(str[left])) {
		++left;
	}
	while (right > left && std::isspace(str[right - 1])) {
		--right;
	}

	return str.substr(left, right - left);
}

}  // namespace reindexer
