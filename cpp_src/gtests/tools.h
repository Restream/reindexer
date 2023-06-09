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
