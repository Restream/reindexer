#include "gtests/tools.h"

#include <algorithm>
#include <vector>
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"

namespace reindexer_tests_tools {
namespace {

constexpr std::string_view kHexChars = "0123456789aAbBcCdDeEfF";
constexpr std::string_view kNilUUID = "00000000-0000-0000-0000-000000000000";
constexpr unsigned kUuidDelimPositions[] = {8, 13, 18, 23};

bool isUuidDelimPos(unsigned i) noexcept {
	return std::find(std::begin(kUuidDelimPositions), std::end(kUuidDelimPositions), i) != std::end(kUuidDelimPositions);
}

template <typename Fn>
reindexer::VariantArray randUuidArrayImpl(Fn fillFn, size_t min, size_t max) {
	assert(min <= max);
	reindexer::VariantArray ret;
	const size_t count = min == max ? min : min + rand() % (max - min);
	ret.reserve(count);
	for (size_t i = 0; i < count; ++i) {
		fillFn(ret);
	}
	return ret;
}

}  // namespace

std::string randStrUuid() {
	if (rand() % 1000 == 0) {
		return std::string{kNilUUID};
	}
	std::string strUuid;
	strUuid.reserve(reindexer::Uuid::kStrFormLen);
	for (size_t i = 0; i < reindexer::Uuid::kStrFormLen; ++i) {
		if (isUuidDelimPos(i)) {
			strUuid.push_back('-');
		} else if (i == 19) {
			strUuid.push_back(kHexChars[8 + rand() % (kHexChars.size() - 8)]);
		} else {
			strUuid.push_back(kHexChars[rand() % kHexChars.size()]);
		}
	}
	return strUuid;
}

reindexer::Uuid randUuid() { return reindexer::Uuid{randStrUuid()}; }

reindexer::Uuid nilUuid() { return reindexer::Uuid{kNilUUID}; }

reindexer::VariantArray randUuidArray(size_t min, size_t max) {
	return randUuidArrayImpl([](auto& v) { v.emplace_back(randUuid()); }, min, max);
}

reindexer::VariantArray randStrUuidArray(size_t min, size_t max) {
	return randUuidArrayImpl([](auto& v) { v.emplace_back(randStrUuid()); }, min, max);
}

reindexer::VariantArray randHeterogeneousUuidArray(size_t min, size_t max) {
	return randUuidArrayImpl(
		[](auto& v) {
			if (rand() % 2) {
				v.emplace_back(randStrUuid());
			} else {
				v.emplace_back(randUuid());
			}
		},
		min, max);
}

MinMaxArgs minMaxArgs(CondType cond, size_t max) {
	MinMaxArgs res;
	switch (cond) {
		case CondEq:
		case CondSet:
		case CondAllSet:
			res.min = 0;
			res.max = max;
			break;
		case CondLike:
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			res.min = res.max = 1;
			break;
		case CondRange:
			res.min = res.max = 2;
			break;
		case CondAny:
		case CondEmpty:
			res.min = res.max = 0;
			break;
		case CondDWithin:
		case CondKnn:
			assert(0);
	}
	return res;
}

reindexer::Point randPoint(long long range) { return reindexer::Point{randBin<double>(-range, range), randBin<double>(-range, range)}; }

const gason::JsonNode& findJsonField(const gason::JsonNode& json, std::string_view fieldName) {
	using namespace std::string_view_literals;
	std::vector<std::string_view> fields;
	std::ignore = reindexer::split(fieldName, "."sv, false, fields);
	assertrx(!fields.empty());
	const auto* node = &json;
	for (auto it = fields.begin(); it != fields.end() - 1; ++it) {
		node = &(*node)[*it];
		if (!node->isObject()) {
			const static auto emptyNode = gason::JsonNode::EmptyNode();
			return emptyNode;
		}
	}
	return (*node)[fields.back()];
}

reindexer::VectorMetric randMetric() noexcept {
	switch (rand() % 3) {
		case 0:
			return reindexer::VectorMetric::Cosine;
		case 1:
			return reindexer::VectorMetric::InnerProduct;
		case 2:
		default:
			return reindexer::VectorMetric::L2;
	}
}

std::string makeLikePattern(std::string_view utf8Str) {
	std::u16string utf16Str = reindexer::utf8_to_utf16(utf8Str);
	for (char16_t& ch : utf16Str) {
		if (rand() % 4 == 0) {
			ch = u'_';
		}
	}
	std::u16string result;
	if (rand() % 4 == 0) {
		result += u'%';
	}
	std::u16string::size_type next = rand() % (utf16Str.size() + 1);
	std::u16string::size_type last = next;
	for (std::u16string::size_type current = 0; current < utf16Str.size();) {
		if (current < next) {
			result += utf16Str.substr(current, next - current);
			last = next;
			current = (rand() % (utf16Str.size() - last + 1)) + last;
		}
		next = (rand() % (utf16Str.size() - current + 1)) + current;
		if (current > last || rand() % 4 == 0) {
			result += u'%';
		}
	}
	if (rand() % 4 == 0) {
		result += u'%';
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

}  // namespace reindexer_tests_tools
