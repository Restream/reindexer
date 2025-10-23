#include "json_helpers.h"
#include "tools/stringstools.h"

namespace json_helpers::impl {
std::string::size_type findField(const std::string& str, const char* fieldName, std::string::size_type pos) {
	return str.find('"' + std::string(fieldName) + "\":", pos);
}

std::string::size_type findFieldValueStart(const std::string& str, std::string::size_type pos) {
	assertrx(pos + 1 < str.size());
	pos = str.find("\":", pos + 1);
	assertrx(pos != std::string::npos);
	assertrx(pos + 2 < str.size());
	pos = str.find_first_not_of(" \t\n", pos + 2);
	assertrx(pos != std::string::npos);
	return pos;
}

template <>
std::string readFieldValue<std::string>(const std::string& str, std::string::size_type pos) {
	pos = findFieldValueStart(str, pos);
	assertrx(str[pos] == '"');
	++pos;
	assertrx(pos < str.size());
	const std::string::size_type end = str.find_first_of("\" ", pos);
	assertrx(end != std::string::npos);
	return str.substr(pos, end - pos);
}

template <>
bool readFieldValue<bool>(const std::string& str, std::string::size_type pos) {
	pos = findFieldValueStart(str, pos);
	if (reindexer::checkIfStartsWith("true", str.substr(pos))) {
		return true;
	} else if (reindexer::checkIfStartsWith("false", str.substr(pos))) {
		return false;
	} else {
		assertrx(0);
	}
	return false;
}

template <>
int readFieldValue<int>(const std::string& str, std::string::size_type pos) {
	pos = findFieldValueStart(str, pos);
	const std::string::size_type end = str.find_first_not_of("+-0123456789", pos);
	assertrx(end != std::string::npos);
	assertrx(end != pos);
	return std::stoi(str.substr(pos, end - pos));
}

template <>
int64_t readFieldValue<int64_t>(const std::string& str, std::string::size_type pos) {
	pos = findFieldValueStart(str, pos);
	const std::string::size_type end = str.find_first_not_of("+-0123456789", pos);
	assertrx(end != std::string::npos);
	assertrx(end != pos);
	return std::stoll(str.substr(pos, end - pos));
}

std::vector<std::string> adoptValuesType(std::initializer_list<const char*> values) {
	std::vector<std::string> result;
	result.reserve(values.size());
	for (const char* v : values) {
		result.emplace_back(v);
	}
	return result;
}
}  // namespace json_helpers::impl
