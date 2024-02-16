#pragma once

#include <time.h>
#include <cctype>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include "core/indexopts.h"
#include "core/keyvalue/variant.h"
#include "core/type_consts.h"
#include "tools/customhash.h"
#include "tools/customlocal.h"
#include "tools/errors.h"

namespace reindexer {

std::string escapeString(std::string_view str);
std::string unescapeString(std::string_view str);
KeyValueType detectValueType(std::string_view value);
Variant stringToVariant(std::string_view value);

[[nodiscard]] RX_ALWAYS_INLINE bool isalpha(char c) noexcept { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'); }
[[nodiscard]] RX_ALWAYS_INLINE bool isdigit(char c) noexcept { return (c >= '0' && c <= '9'); }
[[nodiscard]] RX_ALWAYS_INLINE char tolower(char c) noexcept { return (c >= 'A' && c <= 'Z') ? c + 'a' - 'A' : c; }
std::string toLower(std::string_view src);
inline std::string_view skipSpace(std::string_view str) {
	size_t i = 0;
	for (; i < str.size() && std::isspace(str[i]); ++i)
		;
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
			if (pos != lastPos || !trimEmpty) tokens.push_back(str.substr(lastPos, pos - lastPos));
			break;
		} else if (pos != lastPos || !trimEmpty) {
			tokens.push_back(str.substr(lastPos, pos - lastPos));
		}
	}
	return tokens;
}

void split(const std::string& utf8Str, std::wstring& utf16str, std::vector<std::wstring>& words);
void split(std::string_view utf8Str, std::wstring& utf16str, std::vector<std::wstring>& words, const std::string& extraWordSymbols);
void split(std::string_view str, std::string& buf, std::vector<const char*>& words, const std::string& extraWordSymbols);
[[nodiscard]] size_t calcUtf8After(std::string_view s, size_t limit) noexcept;
[[nodiscard]] std::pair<size_t, size_t> calcUtf8AfterDelims(std::string_view str, size_t limit, std::string_view delims) noexcept;
[[nodiscard]] size_t calcUtf8Before(const char* str, int pos, size_t limit) noexcept;
[[nodiscard]] std::pair<size_t, size_t> calcUtf8BeforeDelims(const char* str, int pos, size_t limit, std::string_view delims) noexcept;

int getUTF8StringCharactersCount(std::string_view str) noexcept;

class Word2PosHelper {
public:
	Word2PosHelper(std::string_view data, const std::string& extraWordSymbols) noexcept
		: data_(data), lastWordPos_(0), lastOffset_(0), extraWordSymbols_(extraWordSymbols) {}
	std::pair<int, int> convert(int wordPos, int endPos);

protected:
	std::string_view data_;
	int lastWordPos_, lastOffset_;
	const std::string& extraWordSymbols_;
};

struct TextOffset {
	int byte = 0;
	int ch = 0;
};

struct WordPositionEx {
	TextOffset start;
	TextOffset end;
	void SetBytePosition(int s, int e) noexcept {
		start.byte = s;
		end.byte = e;
	}
	[[nodiscard]] int StartByte() const noexcept { return start.byte; }
	[[nodiscard]] int EndByte() const noexcept { return end.byte; }
};

struct WordPosition {
	int start;
	int end;
	void SetBytePosition(int s, int e) noexcept {
		start = s;
		end = e;
	}
	[[nodiscard]] int StartByte() const noexcept { return start; }
	[[nodiscard]] int EndByte() const noexcept { return end; }
};

template <typename Pos>
[[nodiscard]] Pos wordToByteAndCharPos(std::string_view str, int wordPosition, const std::string& extraWordSymbols);

template <CollateMode collateMode>
[[nodiscard]] int collateCompare(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable& sortOrderTable) noexcept;
template <>
[[nodiscard]] int collateCompare<CollateASCII>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
template <>
[[nodiscard]] int collateCompare<CollateUTF8>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
template <>
[[nodiscard]] int collateCompare<CollateNumeric>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
template <>
[[nodiscard]] int collateCompare<CollateCustom>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
template <>
[[nodiscard]] int collateCompare<CollateNone>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept;
[[nodiscard]] inline int collateCompare(std::string_view lhs, std::string_view rhs, const CollateOpts& collateOpts) noexcept {
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
			return collateCompare<CollateNone>(lhs, rhs, collateOpts.sortOrderTable);
	}
	return collateCompare<CollateNone>(lhs, rhs, collateOpts.sortOrderTable);
}

std::wstring utf8_to_utf16(std::string_view src);
std::string utf16_to_utf8(const std::wstring& src);
std::wstring& utf8_to_utf16(std::string_view src, std::wstring& dst);
std::string& utf16_to_utf8(const std::wstring& src, std::string& dst);

inline void check_for_replacement(wchar_t& ch) noexcept {
	ch = (ch == 0x451) ? 0x435 : ch;  // 'ё' -> 'е'
}
inline void check_for_replacement(uint32_t& ch) noexcept {
	ch = (ch == 0x451) ? 0x435 : ch;  // 'ё' -> 'е'
}
inline bool is_number(std::string_view str) noexcept {
	uint16_t i = 0;
	for (; (i < str.length() && IsDigit(str[i])); ++i)
		;
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

[[nodiscard]] bool validateObjectName(std::string_view name, bool allowSpecialChars) noexcept;
[[nodiscard]] bool validateUserNsName(std::string_view name) noexcept;
RX_ALWAYS_INLINE bool isSystemNamespaceNameFast(std::string_view name) noexcept { return !name.empty() && name[0] == '#'; }
LogLevel logLevelFromString(std::string_view strLogLevel) noexcept;
std::string_view logLevelToString(LogLevel level) noexcept;
StrictMode strictModeFromString(std::string_view strStrictMode);
std::string_view strictModeToString(StrictMode mode);

inline bool iequals(std::string_view lhs, std::string_view rhs) noexcept {
	if (lhs.size() != rhs.size()) return false;
	for (auto itl = lhs.begin(), itr = rhs.begin(); itl != lhs.end() && itr != rhs.end();) {
		if (tolower(*itl++) != tolower(*itr++)) return false;
	}
	return true;
}
inline bool iless(std::string_view lhs, std::string_view rhs) noexcept {
	const auto len = std::min(lhs.size(), rhs.size());
	for (size_t i = 0; i < len; ++i) {
		if (const auto l = tolower(lhs[i]), r = tolower(rhs[i]); l != r) {
			return l < r;
		}
	}
	return lhs.size() < rhs.size();
}

enum class CaseSensitive : bool { No, Yes };
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
bool endsWith(std::string const& source, std::string_view ending) noexcept;
std::string& ensureEndsWith(std::string& source, std::string_view ending);

Error cursosPosToBytePos(std::string_view str, size_t line, size_t charPos, size_t& bytePos);

std::string randStringAlph(size_t len);

struct nocase_equal_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return iequals(lhs, rhs); }
	bool operator()(std::string_view lhs, const std::string& rhs) const noexcept { return iequals(lhs, rhs); }
	bool operator()(const std::string& lhs, std::string_view rhs) const noexcept { return iequals(lhs, rhs); }
	bool operator()(const std::string& lhs, const std::string& rhs) const noexcept { return iequals(lhs, rhs); }
};

struct nocase_less_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return iless(lhs, rhs); }
	bool operator()(std::string_view lhs, const std::string& rhs) const noexcept { return iless(lhs, rhs); }
	bool operator()(const std::string& lhs, std::string_view rhs) const noexcept { return iless(lhs, rhs); }
	bool operator()(const std::string& lhs, const std::string& rhs) const noexcept { return iless(lhs, rhs); }
};

struct nocase_hash_str {
	using is_transparent = void;

	size_t operator()(std::string_view hs) const noexcept { return collateHash<CollateASCII>(hs); }
	size_t operator()(const std::string& hs) const noexcept { return collateHash<CollateASCII>(hs); }
};

struct less_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return lhs < rhs; }
	bool operator()(std::string_view lhs, const std::string& rhs) const noexcept { return lhs < std::string_view(rhs); }
	bool operator()(const std::string& lhs, std::string_view rhs) const noexcept { return std::string_view(lhs) < rhs; }
	bool operator()(const std::string& lhs, const std::string& rhs) const noexcept { return lhs < rhs; }
};

struct equal_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return lhs == rhs; }
	bool operator()(std::string_view lhs, const std::string& rhs) const noexcept { return lhs == rhs; }
	bool operator()(const std::string& lhs, std::string_view rhs) const noexcept { return rhs == lhs; }
	bool operator()(const std::string& lhs, const std::string& rhs) const noexcept { return lhs == rhs; }
};

struct hash_str {
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
RX_ALWAYS_INLINE bool isTmpNamespaceNameFast(std::string_view name) noexcept { return !name.empty() && name[0] == kTmpNsPrefix; }
[[nodiscard]] inline std::string createTmpNamespaceName(std::string_view baseName) {
	return std::string({kTmpNsPrefix}).append(baseName).append(kTmpNsSuffix).append(randStringAlph(kTmpNsPostfixLen));
}
[[nodiscard]] inline std::string_view demangleTmpNamespaceName(std::string_view tmpNsName) noexcept {
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
