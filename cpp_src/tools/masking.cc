#include "masking.h"
#include <array>
#include "fmt/format.h"

namespace reindexer {
static constexpr auto initCRC32Table() {
	std::array<unsigned int, 256> table{};
	for (int i = 0; i < 256; i++) {
		unsigned int crc = i;
		for (int j = 0; j < 8; j++) {
			crc = crc & 1 ? (crc >> 1) ^ 0xEDB88320UL : crc >> 1;
		}
		table[i] = crc;
	};
	return table;
}
static constexpr auto CRC32Table = initCRC32Table();

static unsigned int CRC32(const char* buf, unsigned long len) noexcept {
	unsigned int crc = 0xFFFFFFFFUL;
	while (len--) {
		crc = CRC32Table[(crc ^ *reinterpret_cast<const unsigned char*>(buf++)) & 0xFF] ^ (crc >> 8);
	}
	return crc ^ 0xFFFFFFFFUL;
}

std::string maskLogin(std::string_view login) {
	if (login.empty()) {
		return {};
	}

	auto offset = (login.length() < 5) ? 0 : 2;
	const size_t maksedLen = offset ? 7 : 3;
	std::string res(maksedLen, '.');
	if (offset) {
		memcpy(res.data(), login.data(), offset);
		memcpy(res.data() + maksedLen - offset, login.data() + login.length() - offset, offset);
	}

	return res;
}

std::string maskPassword(std::string_view password) { return fmt::format("{:#x}", CRC32(password.data(), password.length())); }
}  // namespace reindexer
