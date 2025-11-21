#pragma once

#include <cctype>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include "core/indexopts.h"
#include "core/keyvalue/variant.h"
#include "core/type_consts.h"
#include "estl/comparation_result.h"
#include "tools/customhash.h"
#include "tools/customlocal.h"
#include "tools/errors.h"

// Defined in venored libs
char* u32toax(uint32_t value, char* buffer, int n);

namespace reindexer {

extern const char* kDefaultExtraWordsSymbols;
extern const char* kDefaultWordPartDelimiters;

template <std::input_iterator InputIterator, std::output_iterator<char> OutputIterator>
OutputIterator escapeString(InputIterator first, InputIterator last, OutputIterator result, AddQuotes add_quotes) {
	for (; first != last; ++first) {
		unsigned char ch = static_cast<unsigned char>(*first);
		switch (ch) {
			case '\b':
				*result++ = '\\';
				*result++ = 'b';
				break;
			case '\f':
				*result++ = '\\';
				*result++ = 'f';
				break;
			case '\n':
				*result++ = '\\';
				*result++ = 'n';
				break;
			case '\r':
				*result++ = '\\';
				*result++ = 'r';
				break;
			case '\t':
				*result++ = '\\';
				*result++ = 't';
				break;
			case '\\':
				*result++ = '\\';
				*result++ = '\\';
				break;
			case '"':
				*result++ = '\\';
				*result++ = '"';
				if (add_quotes == AddQuotes_True) {
					*result++ = '"';
				}
				break;
			default:
				if (ch < 0x20) {
					char hex[5];
					char* end = u32toax(ch, hex, 4);
					*result++ = '\\';
					*result++ = 'u';
					result = std::copy(hex, end, result);
				} else {
					*result++ = ch;
				}
		}
	}
	return result;
}

std::string escapeString(std::string_view str);
std::string unescapeString(std::string_view str);
KeyValueType detectValueType(std::string_view value);
Variant stringToVariant(std::string_view value);

RX_ALWAYS_INLINE constexpr bool isalpha(char c) noexcept { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'); }
RX_ALWAYS_INLINE constexpr bool isdigit(char c) noexcept { return (c >= '0' && c <= '9'); }
RX_ALWAYS_INLINE constexpr bool issign(char c) noexcept { return (c == '+' || c == '-'); }
RX_ALWAYS_INLINE constexpr char tolower(char c) noexcept { return (c >= 'A' && c <= 'Z') ? c + 'a' - 'A' : c; }
std::string toLower(std::string_view src);
inline std::string_view skipSpace(std::string_view str) {
	size_t i = 0;
	for (; i < str.size() && std::isspace(str[i]); ++i);
	return str.substr(i);
}

template <typename Str>
bool strEmpty(const Str& str) noexcept {
	return str.empty();
}
inline bool strEmpty(const char* str) noexcept { return str[0] == '\0'; }

template <typename Container>
Container& split(const typename Container::value_type& str, std::string_view delimiters, bool trimEmpty, Container& tokens) {
	tokens.resize(0);

	for (size_t pos, lastPos = 0;; lastPos = pos + 1) {
		pos = str.find_first_of(delimiters, lastPos);
		if (pos == Container::value_type::npos) {
			pos = str.length();
			if (pos != lastPos || !trimEmpty) {
				tokens.push_back(str.substr(lastPos, pos - lastPos));
			}
			break;
		} else if (pos != lastPos || !trimEmpty) {
			tokens.push_back(str.substr(lastPos, pos - lastPos));
		}
	}
	return tokens;
}

struct [[nodiscard]] SplitOptions {
public:
	SplitOptions() { SetSymbols(kDefaultExtraWordsSymbols, kDefaultWordPartDelimiters); }

	bool HasDelims() const noexcept { return hasDelims_; }

	bool IsWordPartDelimiter(uint32_t ch) const noexcept {
		if (ch >= wordPartDelimitersMask_.size()) {
			return false;
		}
		return wordPartDelimitersMask_[ch];
	}
	bool IsWordSymbol(uint32_t ch) const noexcept { return IsAlpha(ch) || IsDigit(ch) || isExtraWordSymbol(ch); }

	bool IsWord(std::string_view str) const noexcept;

	bool ContainsDelims(const std::string_view str) const;
	bool ContainsDelims(const std::wstring_view str) const;
	void RemoveDelims(std::string_view str, std::string& res) const;
	std::string RemoveDelims(std::string_view str) const;
	std::string RemoveAccentsAndDiacritics(std::string_view str) const;

	bool NeedToRemoveDiacritics(wchar_t ch) const noexcept { return FitsMask(ch, removeDiacriticsMask_); }

	bool operator==(const SplitOptions& rhs) const noexcept = default;

	bool operator!=(const SplitOptions& rhs) const noexcept { return !(*this == rhs); }

	void SetSymbols(std::string_view extraWordSymbols, std::string_view wordPartDelimiters);
	void SetMinPartSize(size_t minPartSize) noexcept { minPartSize_ = minPartSize > 0 ? minPartSize : 1; }
	void SetRemoveDiacriticsMask(SymbolTypeMask removeDiacriticsMask) noexcept { removeDiacriticsMask_ = removeDiacriticsMask; }

	SymbolTypeMask GetRemoveDiacriticsMask() const noexcept { return removeDiacriticsMask_; }
	size_t GetMinPartSize() const noexcept { return minPartSize_; }

	std::string GetExtraWordSymbols() const;
	std::string GetWordPartDelimiters() const;

private:
	bool isExtraWordSymbol(uint32_t ch) const noexcept {
		if (ch >= extraWordSymbolsMask_.size()) {
			return false;
		}
		return extraWordSymbolsMask_[ch];
	}

	SymbolTypeMask removeDiacriticsMask_ = kRemoveAllDiacriticsMask;
	size_t minPartSize_ = 3;

	std::vector<bool> extraWordSymbolsMask_;
	std::vector<bool> wordPartDelimitersMask_;
	bool hasDelims_ = false;
};

struct [[nodiscard]] WordWithPos {
	std::string_view word;
	size_t pos = 0;

	WordWithPos() = default;
	WordWithPos(std::string_view word, size_t pos) : word(word), pos(pos) {}
};

void split(const std::string& utf8Str, std::wstring& utf16str, std::vector<std::wstring>& words);
void split(std::string_view utf8Str, std::wstring& utf16str, std::vector<std::wstring>& words, const SplitOptions& options);
void split(std::string_view str, std::string& buf, std::vector<WordWithPos>& words, const SplitOptions& options);
size_t calcUtf8After(std::string_view s, size_t limit) noexcept;
std::pair<size_t, size_t> calcUtf8AfterDelims(std::string_view str, size_t limit, std::string_view delims) noexcept;
size_t calcUtf8Before(const char* str, int pos, size_t limit) noexcept;
std::pair<size_t, size_t> calcUtf8BeforeDelims(const char* str, int pos, size_t limit, std::string_view delims) noexcept;

std::string_view trimSpaces(std::string_view str) noexcept;

int getUTF8StringCharactersCount(std::string_view str) noexcept;

struct [[nodiscard]] TextOffset {
	int byte = 0;
	int ch = 0;
};

struct [[nodiscard]] WordPositionEx {
	TextOffset start;
	TextOffset end;
	void SetBytePosition(int s, int e) noexcept {
		start.byte = s;
		end.byte = e;
	}
	int StartByte() const noexcept { return start.byte; }
	int EndByte() const noexcept { return end.byte; }
};

struct [[nodiscard]] WordPosition {
	int start;
	int end;
	void SetBytePosition(int s, int e) noexcept {
		start = s;
		end = e;
	}
	int StartByte() const noexcept { return start; }
	int EndByte() const noexcept { return end; }
};

template <CollateMode collateMode>
ComparationResult collateCompare(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable& sortOrderTable) noexcept;
template <>
ComparationResult collateCompare<CollateASCII>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
template <>
ComparationResult collateCompare<CollateUTF8>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
template <>
ComparationResult collateCompare<CollateNumeric>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
template <>
ComparationResult collateCompare<CollateCustom>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
template <>
ComparationResult collateCompare<CollateNone>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
inline ComparationResult collateCompare(std::string_view lhs, std::string_view rhs, const CollateOpts& collateOpts) noexcept {
	switch (collateOpts.mode) {
		case CollateASCII:
			return collateCompare<CollateASCII>(lhs, rhs, collateOpts.sortOrderTable);
		case CollateUTF8:
			return collateCompare<CollateUTF8>(lhs, rhs, collateOpts.sortOrderTable);
		case CollateNumeric:
			return collateCompare<CollateNumeric>(lhs, rhs, collateOpts.sortOrderTable);
		case CollateCustom:
			return collateCompare<CollateCustom>(lhs, rhs, collateOpts.sortOrderTable);
		case CollateNone:
		default:
			return collateCompare<CollateNone>(lhs, rhs, collateOpts.sortOrderTable);
	}
}

std::wstring utf8_to_utf16(std::string_view src);
std::string utf16_to_utf8(const std::wstring_view src);
size_t utf16_to_utf8_size(const std::wstring_view src);
void utf8_to_utf16(std::string_view src, std::wstring& dst);
void utf16_to_utf8(const std::wstring_view src, std::string& dst);

inline bool is_number(std::string_view str) noexcept {
	uint16_t i = 0;
	for (; (i < str.length() && IsDigit(str[i])); ++i);
	return (i && i == str.length());
}

int fast_strftime(char* buf, const tm* tm);
std::string urldecode2(std::string_view str);

int stoi(std::string_view sl);
std::optional<int> try_stoi(std::string_view sl);
int64_t stoll(std::string_view sl);
int double_to_str(double v, char* buf, int capacity);
int double_to_str_no_trailing(double v, char* buf, int capacity);
std::string double_to_str(double v);
int float_to_str(float v, char* buf, int capacity);
int float_to_str_no_trailing(float v, char* buf, int capacity);
std::string float_to_str(float v);
void float_vector_to_str(ConstFloatVectorView view, WrSerializer& ser);
std::string float_vector_to_str(ConstFloatVectorView view);

bool validateObjectName(std::string_view name, bool allowSpecialChars) noexcept;
bool validateUserNsName(std::string_view name) noexcept;
RX_ALWAYS_INLINE bool isSystemNamespaceNameFast(std::string_view name) noexcept { return !name.empty() && (name[0] == '#'); }
RX_ALWAYS_INLINE bool isSystemNamespaceNameFastReplication(std::string_view name) noexcept {
	return !name.empty() && (name[0] == '#' || name[0] == '@' || name[0] == '!');
}
LogLevel logLevelFromString(std::string_view strLogLevel) noexcept;
std::string_view logLevelToString(LogLevel level) noexcept;
StrictMode strictModeFromString(std::string_view strStrictMode);
std::string_view strictModeToString(StrictMode mode);

inline constexpr bool iequals(std::string_view lhs, std::string_view rhs) noexcept {
	if (lhs.size() != rhs.size()) {
		return false;
	}
	for (auto itl = lhs.begin(), itr = rhs.begin(); itl != lhs.end() && itr != rhs.end();) {
		if (tolower(*itl++) != tolower(*itr++)) {
			return false;
		}
	}
	return true;
}
inline constexpr bool iless(std::string_view lhs, std::string_view rhs) noexcept {
	const auto len = std::min(lhs.size(), rhs.size());
	for (size_t i = 0; i < len; ++i) {
		if (const auto l = tolower(lhs[i]), r = tolower(rhs[i]); l != r) {
			return l < r;
		}
	}
	return lhs.size() < rhs.size();
}

enum class [[nodiscard]] CaseSensitive : bool { No, Yes };
template <CaseSensitive sensitivity>
bool checkIfStartsWith(std::string_view pattern, std::string_view src) noexcept;
RX_ALWAYS_INLINE bool checkIfStartsWith(std::string_view pattern, std::string_view src) noexcept {
	return checkIfStartsWith<CaseSensitive::No>(pattern, src);
}
RX_ALWAYS_INLINE bool checkIfStartsWithCS(std::string_view pattern, std::string_view src) noexcept {
	return checkIfStartsWith<CaseSensitive::Yes>(pattern, src);
}

template <CaseSensitive sensitivity>
bool checkIfEndsWith(std::string_view pattern, std::string_view src) noexcept;
RX_ALWAYS_INLINE bool checkIfEndsWith(std::string_view pattern, std::string_view src) noexcept {
	return checkIfEndsWith<CaseSensitive::No>(pattern, src);
}

bool isPrintable(std::string_view str) noexcept;
bool isBlank(std::string_view token) noexcept;
bool endsWith(const std::string& source, std::string_view ending) noexcept;
std::string& ensureEndsWith(std::string& source, std::string_view ending);

Error cursorPosToBytePos(std::string_view str, size_t line, size_t charPos, size_t& bytePos);
void charMultilinePos(std::string_view str, size_t pos, size_t search_start, size_t& line, size_t& col) noexcept;

std::string randStringAlph(size_t len);

struct [[nodiscard]] nocase_equal_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return iequals(lhs, rhs); }
	bool operator()(std::string_view lhs, const std::string& rhs) const noexcept { return iequals(lhs, rhs); }
	bool operator()(const std::string& lhs, std::string_view rhs) const noexcept { return iequals(lhs, rhs); }
	bool operator()(const std::string& lhs, const std::string& rhs) const noexcept { return iequals(lhs, rhs); }
};

struct [[nodiscard]] nocase_less_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return iless(lhs, rhs); }
	bool operator()(std::string_view lhs, const std::string& rhs) const noexcept { return iless(lhs, rhs); }
	bool operator()(const std::string& lhs, std::string_view rhs) const noexcept { return iless(lhs, rhs); }
	bool operator()(const std::string& lhs, const std::string& rhs) const noexcept { return iless(lhs, rhs); }
};

struct [[nodiscard]] nocase_hash_str {
	using is_transparent = void;

	size_t operator()(std::string_view hs) const noexcept { return collateHash<CollateASCII>(hs); }
	size_t operator()(const std::string& hs) const noexcept { return collateHash<CollateASCII>(hs); }
};

struct [[nodiscard]] less_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return lhs < rhs; }
	bool operator()(std::string_view lhs, const std::string& rhs) const noexcept { return lhs < std::string_view(rhs); }
	bool operator()(const std::string& lhs, std::string_view rhs) const noexcept { return std::string_view(lhs) < rhs; }
	bool operator()(const std::string& lhs, const std::string& rhs) const noexcept { return lhs < rhs; }
};

struct [[nodiscard]] equal_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return lhs == rhs; }
	bool operator()(std::string_view lhs, const std::string& rhs) const noexcept { return lhs == rhs; }
	bool operator()(const std::string& lhs, std::string_view rhs) const noexcept { return rhs == lhs; }
	bool operator()(const std::string& lhs, const std::string& rhs) const noexcept { return lhs == rhs; }
};

struct [[nodiscard]] hash_str {
	using is_transparent = void;

	size_t operator()(std::string_view hs) const noexcept { return collateHash<CollateNone>(hs); }
	size_t operator()(const std::string& hs) const noexcept { return collateHash<CollateNone>(hs); }
};

RX_ALWAYS_INLINE void deepCopy(std::string& dst, const std::string& src) {
	dst.resize(src.size());
	std::memcpy(&dst[0], &src[0], src.size());
}

constexpr size_t kTmpNsPostfixLen = 20;
constexpr std::string_view kTmpNsSuffix = "_tmp_";
constexpr char kTmpNsPrefix = '@';
RX_ALWAYS_INLINE bool isTmpNamespaceName(std::string_view name) noexcept { return !name.empty() && name[0] == kTmpNsPrefix; }
inline std::string createTmpNamespaceName(std::string_view baseName) {
	return std::string({kTmpNsPrefix}).append(baseName).append(kTmpNsSuffix).append(randStringAlph(kTmpNsPostfixLen));
}
inline std::string_view demangleTmpNamespaceName(std::string_view tmpNsName) noexcept {
	if (tmpNsName.size() < kTmpNsPostfixLen + kTmpNsSuffix.size() + 1) {
		return tmpNsName;
	}
	if (tmpNsName[0] != kTmpNsPrefix) {
		return tmpNsName;
	}
	if (tmpNsName.substr(tmpNsName.size() - kTmpNsPostfixLen - kTmpNsSuffix.size(), kTmpNsSuffix.size()) != kTmpNsSuffix) {
		return tmpNsName;
	}
	return tmpNsName.substr(1, tmpNsName.size() - kTmpNsPostfixLen - 1 - kTmpNsSuffix.size());
}

}  // namespace reindexer
