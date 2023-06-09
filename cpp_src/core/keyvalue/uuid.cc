#include "uuid.h"

#include "estl/span.h"

namespace reindexer {

Uuid::Uuid(std::string_view str) : data_{0, 0} {
	const auto err = tryParse(str, data_);
	if (!err.ok()) {
		throw err;
	}
}

Error Uuid::tryParse(std::string_view str, uint64_t (&data)[2]) noexcept {
	unsigned i = 0;
	for (char ch : str) {
		if (i >= 32 && ch != '-') {
			return Error(errNotValid, "UUID should consist of 32 hexadecimal digits: '%s'", str);
		}
		uint64_t value;
		switch (ch) {
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				value = ch - '0';
				break;
			case 'a':
			case 'A':
				value = 0xA;
				break;
			case 'b':
			case 'B':
				value = 0xB;
				break;
			case 'c':
			case 'C':
				value = 0xC;
				break;
			case 'd':
			case 'D':
				value = 0xD;
				break;
			case 'e':
			case 'E':
				value = 0xE;
				break;
			case 'f':
			case 'F':
				value = 0xF;
				break;
			case '-':
				continue;
			default:
				return Error(errNotValid, "UUID cannot contain char '%c': '%s'", ch, str);
		}
		data[i / 16] = (data[i / 16] << 4) | value;
		++i;
	}
	if (i != 32) {
		return Error(errNotValid, "UUID should consist of 32 hexadecimal digits: '%s'", str);
	}
	if ((data[0] != 0 || data[1] != 0) && (data[1] >> 63) == 0) {
		return Error(errNotValid, "Variant 0 of UUID is unsupported: '%s'", str);
	}
	return {};
}

std::optional<Uuid> Uuid::TryParse(std::string_view str) noexcept {
	Uuid ret;
	const auto err = tryParse(str, ret.data_);
	if (err.ok()) {
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

void Uuid::PutToStr(span<char> str) const noexcept {
	assertrx(str.size() >= kStrFormLen);
	static constexpr char ch[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	for (size_t i = 0, j = 0; i < kStrFormLen; ++i) {
		switch (i) {
			case 8:
			case 13:
			case 18:
			case 23:
				str[i] = '-';
				break;
			default:
				str[i] = ch[(data_[j / 16] >> ((15 - j % 16) * 4)) & 0xF];
				++j;
				break;
		}
	}
}

}  // namespace reindexer
