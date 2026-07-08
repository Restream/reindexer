#include "uuid.h"

#include <span>

namespace reindexer {

Uuid::Uuid(std::string_view str) : data_{0, 0} {
	const auto err = tryParse(str, data_);
	if (!err.ok()) [[unlikely]] {
		throw err;
	}
}

#ifdef GET_NUM
static_assert(false, "GET_NUM is already defined");
#endif

#define GET_NUM(i)                                                                         \
	num = hexCharToNum[static_cast<unsigned char>(str[i])];                                \
	if (num > 15) [[unlikely]] {                                                           \
		if (str[i] == '-') {                                                               \
			return Error(errNotValid, "Invalid UUID format: '{}'", str);                   \
		} else {                                                                           \
			return Error(errNotValid, "UUID cannot contain char '{}': '{}'", str[i], str); \
		}                                                                                  \
	}

Error Uuid::tryParse(std::string_view str, uint64_t (&data)[2]) noexcept {
	static constexpr uint64_t hexCharToNum[] = {
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0,   1,	2,	 3,
		4,	 5,	  6,   7,	8,	 9,	  255, 255, 255, 255, 255, 255, 255, 10,  11,  12,	13,	 14,  15,  255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 10,	11,	 12,  13,  14,	15,	 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255};
	static constexpr size_t kHexDigitsCount = 32;
	uint64_t num;
	switch (str.size()) {
		case kHexDigitsCount:
			GET_NUM(0)
			data[0] = num << 60;
			GET_NUM(1)
			data[0] |= num << 56;
			GET_NUM(2)
			data[0] |= num << 52;
			GET_NUM(3)
			data[0] |= num << 48;
			GET_NUM(4)
			data[0] |= num << 44;
			GET_NUM(5)
			data[0] |= num << 40;
			GET_NUM(6)
			data[0] |= num << 36;
			GET_NUM(7)
			data[0] |= num << 32;
			GET_NUM(8)
			data[0] |= num << 28;
			GET_NUM(9)
			data[0] |= num << 24;
			GET_NUM(10)
			data[0] |= num << 20;
			GET_NUM(11)
			data[0] |= num << 16;
			GET_NUM(12)
			data[0] |= num << 12;
			GET_NUM(13)
			data[0] |= num << 8;
			GET_NUM(14)
			data[0] |= num << 4;
			GET_NUM(15)
			data[0] |= num;
			GET_NUM(16)
			data[1] = num << 60;
			GET_NUM(17)
			data[1] |= num << 56;
			GET_NUM(18)
			data[1] |= num << 52;
			GET_NUM(19)
			data[1] |= num << 48;
			GET_NUM(20)
			data[1] |= num << 44;
			GET_NUM(21)
			data[1] |= num << 40;
			GET_NUM(22)
			data[1] |= num << 36;
			GET_NUM(23)
			data[1] |= num << 32;
			GET_NUM(24)
			data[1] |= num << 28;
			GET_NUM(25)
			data[1] |= num << 24;
			GET_NUM(26)
			data[1] |= num << 20;
			GET_NUM(27)
			data[1] |= num << 16;
			GET_NUM(28)
			data[1] |= num << 12;
			GET_NUM(29)
			data[1] |= num << 8;
			GET_NUM(30)
			data[1] |= num << 4;
			GET_NUM(31)
			data[1] |= num;
			break;
		case kStrFormLen:
			if (str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-') [[unlikely]] {
				return Error(errNotValid, "Invalid UUID format: '{}'", str);
			}
			GET_NUM(0)
			data[0] = num << 60;
			GET_NUM(1)
			data[0] |= num << 56;
			GET_NUM(2)
			data[0] |= num << 52;
			GET_NUM(3)
			data[0] |= num << 48;
			GET_NUM(4)
			data[0] |= num << 44;
			GET_NUM(5)
			data[0] |= num << 40;
			GET_NUM(6)
			data[0] |= num << 36;
			GET_NUM(7)
			data[0] |= num << 32;
			GET_NUM(9)
			data[0] |= num << 28;
			GET_NUM(10)
			data[0] |= num << 24;
			GET_NUM(11)
			data[0] |= num << 20;
			GET_NUM(12)
			data[0] |= num << 16;
			GET_NUM(14)
			data[0] |= num << 12;
			GET_NUM(15)
			data[0] |= num << 8;
			GET_NUM(16)
			data[0] |= num << 4;
			GET_NUM(17)
			data[0] |= num;
			GET_NUM(19)
			data[1] = num << 60;
			GET_NUM(20)
			data[1] |= num << 56;
			GET_NUM(21)
			data[1] |= num << 52;
			GET_NUM(22)
			data[1] |= num << 48;
			GET_NUM(24)
			data[1] |= num << 44;
			GET_NUM(25)
			data[1] |= num << 40;
			GET_NUM(26)
			data[1] |= num << 36;
			GET_NUM(27)
			data[1] |= num << 32;
			GET_NUM(28)
			data[1] |= num << 28;
			GET_NUM(29)
			data[1] |= num << 24;
			GET_NUM(30)
			data[1] |= num << 20;
			GET_NUM(31)
			data[1] |= num << 16;
			GET_NUM(32)
			data[1] |= num << 12;
			GET_NUM(33)
			data[1] |= num << 8;
			GET_NUM(34)
			data[1] |= num << 4;
			GET_NUM(35)
			data[1] |= num;
			break;
		default:
			return Error(errNotValid, "UUID should consist of 32 hexadecimal digits: '{}'", str);
	}
	if ((data[0] != 0 || data[1] != 0) && (data[1] >> 63) == 0) [[unlikely]] {
		return Error(errNotValid, "Variant 0 of UUID is unsupported: '{}'", str);
	}
	return {};
}

#undef GET_NUM

std::optional<Uuid> Uuid::TryParse(std::string_view str) noexcept {
	Uuid ret;
	const auto err = tryParse(str, ret.data_);
	if (err.ok()) [[likely]] {
		return ret;
	} else {
		return std::nullopt;
	}
}

Uuid::operator std::string() const {
	std::string res;
	res.resize(kStrFormLen);
	PutToStr({res.data(), res.size()});
	return res;
}

Uuid::operator key_string() const {
	char res[kStrFormLen];
	PutToStr({res, kStrFormLen});
	return make_key_string(std::string_view(res, kStrFormLen));
}

void Uuid::PutToStr(std::span<char> str) const noexcept {
	assertrx(str.size() >= kStrFormLen);
	static constexpr char hexChars[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	str[0] = hexChars[(data_[0] >> 60) & 0xF];
	str[1] = hexChars[(data_[0] >> 56) & 0xF];
	str[2] = hexChars[(data_[0] >> 52) & 0xF];
	str[3] = hexChars[(data_[0] >> 48) & 0xF];
	str[4] = hexChars[(data_[0] >> 44) & 0xF];
	str[5] = hexChars[(data_[0] >> 40) & 0xF];
	str[6] = hexChars[(data_[0] >> 36) & 0xF];
	str[7] = hexChars[(data_[0] >> 32) & 0xF];
	str[8] = '-';
	str[9] = hexChars[(data_[0] >> 28) & 0xF];
	str[10] = hexChars[(data_[0] >> 24) & 0xF];
	str[11] = hexChars[(data_[0] >> 20) & 0xF];
	str[12] = hexChars[(data_[0] >> 16) & 0xF];
	str[13] = '-';
	str[14] = hexChars[(data_[0] >> 12) & 0xF];
	str[15] = hexChars[(data_[0] >> 8) & 0xF];
	str[16] = hexChars[(data_[0] >> 4) & 0xF];
	str[17] = hexChars[data_[0] & 0xF];
	str[18] = '-';
	str[19] = hexChars[(data_[1] >> 60) & 0xF];
	str[20] = hexChars[(data_[1] >> 56) & 0xF];
	str[21] = hexChars[(data_[1] >> 52) & 0xF];
	str[22] = hexChars[(data_[1] >> 48) & 0xF];
	str[23] = '-';
	str[24] = hexChars[(data_[1] >> 44) & 0xF];
	str[25] = hexChars[(data_[1] >> 40) & 0xF];
	str[26] = hexChars[(data_[1] >> 36) & 0xF];
	str[27] = hexChars[(data_[1] >> 32) & 0xF];
	str[28] = hexChars[(data_[1] >> 28) & 0xF];
	str[29] = hexChars[(data_[1] >> 24) & 0xF];
	str[30] = hexChars[(data_[1] >> 20) & 0xF];
	str[31] = hexChars[(data_[1] >> 16) & 0xF];
	str[32] = hexChars[(data_[1] >> 12) & 0xF];
	str[33] = hexChars[(data_[1] >> 8) & 0xF];
	str[34] = hexChars[(data_[1] >> 4) & 0xF];
	str[35] = hexChars[data_[1] & 0xF];
}

}  // namespace reindexer
