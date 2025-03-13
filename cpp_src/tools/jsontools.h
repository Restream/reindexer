#pragma once

#include <string>
#include <string_view>

#include <span>
#include "gason/gason.h"
#include "tools/stringstools.h"

namespace reindexer {

class WrSerializer;

constexpr int kJsonShiftWidth = 4;

void jsonValueToString(gason::JsonValue o, WrSerializer& ser, int shift = kJsonShiftWidth, int indent = 0, bool escapeStrings = true);
void prettyPrintJSON(std::span<char> json, WrSerializer& ser, int shift = kJsonShiftWidth);
void prettyPrintJSON(std::string_view json, WrSerializer& ser, int shift = kJsonShiftWidth);

std::string stringifyJson(const gason::JsonNode& elem, bool escapeStrings = true);

namespace details {
/**
 * @brief Safely reads JSON value from parent json node, in case of errors, value left unmodified.
 * @tparam T type of value to read
 * @tparam Args  for numeric types can contain typenames for value range limits of type \p T. List is deducible from arguments list.
 * @tparam required  value importance mode. True - item with \p valueName is required and in case of absence function will
 * return an Error(errNotFound).
 * @param parent  parent JSON node \p valueName item belongs to
 * @param valueName  json value name
 * @param value  a reference to variable to write correct value to
 * @param args  a number of optional parameters. For numeric types can contain value range limits [min, max], respectivelly. If
 * ommited then these parameters will be defaulted to <tt>T minv = std::numeric_limits<T>::min(), T maxv =
 * std::numeric_limits<T>::max()</tt>
 * @returns
 * 	- Error{errOK} - \p valueName read successfuly, \p value updated;
 * 	- Error{...} - in case of read errors (inspect Error.what() for details), \p value left unchanged.
 */
template <bool required, typename T, typename JsonT, typename... Args>
Error tryReadJsonValue(std::string* errLog, const gason::JsonNode& parent, std::string_view valueName, T& value, Args&&... args) {
	Error result;
	if (!parent[valueName].empty()) {
		try {
			value = parent[valueName].As<JsonT>(value, std::forward<Args>(args)...);
		} catch (const gason::Exception& ex) {
			result = Error(errParseJson, "{}", ex.what());
		}
	} else if constexpr (required) {
		result = Error(errParseJson, "Required paramenter '{}' is not found.\n", valueName);
	}

	if (errLog && !result.ok()) {
		ensureEndsWith(*errLog, "\n") += result.what();
	}
	return result;
}
}  // namespace details

/// @brief Safely reads optional JSON value
template <typename T, typename... Args>
Error tryReadOptionalJsonValue(std::string* errLog, const gason::JsonNode& parent, std::string_view valueName, T& value, Args&&... args) {
	return details::tryReadJsonValue<false, T, T, Args...>(errLog, parent, valueName, value, std::forward<Args>(args)...);
}
template <typename T, typename... Args>
Error tryReadOptionalJsonValue(std::string* errLog, const gason::JsonNode& parent, std::string_view valueName, std::atomic<T>& value,
							   Args&&... args) {
	return details::tryReadJsonValue<false, std::atomic<T>, T, Args...>(errLog, parent, valueName, value, std::forward<Args>(args)...);
}

/// @brief Safely reads required JSON value
template <typename T, typename... Args>
Error tryReadRequiredJsonValue(std::string* errLog, const gason::JsonNode& parent, std::string_view valueName, T& value, Args&&... args) {
	return details::tryReadJsonValue<true, T, T, Args...>(errLog, parent, valueName, value, std::forward<Args>(args)...);
}
template <typename T, typename... Args>
Error tryReadRequiredJsonValue(std::string* errLog, const gason::JsonNode& parent, std::string_view valueName, std::atomic<T>& value,
							   Args&&... args) {
	return details::tryReadJsonValue<true, std::atomic<T>, T, Args...>(errLog, parent, valueName, value, std::forward<Args>(args)...);
}

}  // namespace reindexer
