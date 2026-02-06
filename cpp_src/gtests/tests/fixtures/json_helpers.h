#pragma once

#include <unordered_set>
#include "gtest/gtest.h"

namespace json_helpers {
namespace impl {
std::string::size_type findField(const std::string& str, const char* fieldName, std::string::size_type pos);
std::string::size_type findFieldValueStart(const std::string& str, std::string::size_type pos);

template <typename T>
T readFieldValue(const std::string&, std::string::size_type);

template <typename T>
std::vector<T> adoptValuesType(std::initializer_list<T> values) {
	return std::vector<T>(values);
}
std::vector<std::string> adoptValuesType(std::initializer_list<const char*> values);

template <>
std::string readFieldValue<std::string>(const std::string&, std::string::size_type);
template <>
bool readFieldValue<bool>(const std::string&, std::string::size_type);
template <>
int readFieldValue<int>(const std::string&, std::string::size_type);
template <>
int64_t readFieldValue<int64_t>(const std::string&, std::string::size_type);
}  // namespace impl

template <typename T>
void AssertJsonFieldEqualTo(const std::string& str, const char* fieldName, std::initializer_list<T> v) {
	const auto values = impl::adoptValuesType(v);
	std::string::size_type pos = impl::findField(str, fieldName, 0);
	size_t i = 0;
	for (auto it = values.begin(); it != values.end(); ++i, ++it) {
		ASSERT_NE(pos, std::string::npos) << str << ": Field '" << fieldName << "' found less than expected (Expected " << values.size()
										  << ')';
		const auto fieldValue = impl::readFieldValue<typename decltype(values)::value_type>(str, pos);
		ASSERT_EQ(*it, fieldValue) << str << ": Field '" << fieldName << "' value number " << i << " missmatch. Expected: '" << *it
								   << "', got '" << fieldValue << '\'';
		pos = impl::findField(str, fieldName, pos + 1);
	}
	ASSERT_EQ(pos, std::string::npos) << str << ": Field '" << fieldName << "' found more then expected (Expected " << values.size() << ')';
}

template <typename T>
void AssertJsonFieldEqualToOneOf(const std::string& str, const char* fieldName, std::initializer_list<std::unordered_set<T>> expectedSets) {
	auto formatSet = [](const std::unordered_set<T>& set) -> std::string {
		std::string result = "{'";
		for (auto it = set.begin(); it != set.end(); ++it) {
			if (it != set.begin()) {
				result += "', '";
			}
			result += *it;
		}
		result += "'}";
		return result;
	};

	const auto allExpectedSets = impl::adoptValuesType(expectedSets);
	std::string::size_type pos = impl::findField(str, fieldName, 0);
	size_t i = 0;

	for (auto it = allExpectedSets.begin(); it != allExpectedSets.end(); ++i, ++it) {
		ASSERT_NE(pos, std::string::npos) << str << ": Field '" << fieldName << "' found less than expected (Expected "
										  << allExpectedSets.size() << ')';

		const T fieldValue = impl::readFieldValue<T>(str, pos);
		const auto& allowedValues = *it;

		ASSERT_TRUE(allowedValues.find(fieldValue) != allowedValues.end())
			<< str << ": Field '" << fieldName << "' value number " << i << " mismatch. Got: '" << fieldValue
			<< "', expected one of: " << formatSet(allowedValues);

		pos = impl::findField(str, fieldName, pos + 1);
	}

	ASSERT_EQ(pos, std::string::npos) << str << ": Field '" << fieldName << "' found more than expected (Expected "
									  << allExpectedSets.size() << ')';
}

template <typename T>
std::vector<T> GetJsonFieldValues(const std::string& str, const char* fieldName) {
	std::vector<T> result;
	std::string::size_type pos = impl::findField(str, fieldName, 0);
	while (pos != std::string::npos) {
		result.push_back(impl::readFieldValue<T>(str, pos));
		pos = impl::findField(str, fieldName, pos + 1);
	}
	return result;
}

inline void AssertJsonFieldAbsent(const std::string& str, const char* fieldName) {
	const std::string::size_type pos = impl::findField(str, fieldName, 0);
	ASSERT_EQ(pos, std::string::npos) << str << ": Field '" << fieldName << "' found";
}

}  // namespace json_helpers
