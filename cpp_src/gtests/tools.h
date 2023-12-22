#include <string>
#include "core/keyvalue/uuid.h"

static constexpr std::string_view hexChars = "0123456789aAbBcCdDeEfF";
static constexpr std::string_view nilUUID = "00000000-0000-0000-0000-000000000000";
static constexpr unsigned uuidDelimPositions[] = {8, 13, 18, 23};

inline bool isUuidDelimPos(unsigned i) noexcept {
	return std::find(std::begin(uuidDelimPositions), std::end(uuidDelimPositions), i) != std::end(uuidDelimPositions);
}

inline std::string randStrUuid() {
	if (rand() % 1000 == 0) return std::string{nilUUID};
	std::string strUuid;
	strUuid.reserve(reindexer::Uuid::kStrFormLen);
	for (size_t i = 0; i < reindexer::Uuid::kStrFormLen; ++i) {
		if (isUuidDelimPos(i)) {
			strUuid.push_back('-');
		} else if (i == 19) {
			strUuid.push_back(hexChars[8 + rand() % (hexChars.size() - 8)]);
		} else {
			strUuid.push_back(hexChars[rand() % hexChars.size()]);
		}
	}
	return strUuid;
}

inline reindexer::Uuid randUuid() { return reindexer::Uuid{randStrUuid()}; }
inline reindexer::Uuid nilUuid() { return reindexer::Uuid{nilUUID}; }

template <typename Fn>
inline reindexer::VariantArray randUuidArrayImpl(Fn fillFn, size_t min, size_t max) {
	assert(min <= max);
	reindexer::VariantArray ret;
	const size_t count = min == max ? min : min + rand() % (max - min);
	ret.reserve(count);
	for (size_t i = 0; i < count; ++i) {
		fillFn(ret);
	}
	return ret;
}

inline reindexer::VariantArray randUuidArray(size_t min, size_t max) {
	return randUuidArrayImpl([](auto& v) { v.emplace_back(randUuid()); }, min, max);
}

inline reindexer::VariantArray randStrUuidArray(size_t min, size_t max) {
	return randUuidArrayImpl([](auto& v) { v.emplace_back(randStrUuid()); }, min, max);
}

inline reindexer::VariantArray randHeterogeneousUuidArray(size_t min, size_t max) {
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

inline auto minMaxArgs(CondType cond, size_t max) {
	struct {
		size_t min;
		size_t max;
	} res;
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
			assert(0);
	}
	return res;
}
