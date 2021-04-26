#pragma once

#include <string_view>

class JoinedSelectorMock {
public:
	JoinedSelectorMock(const char* ns) : ns_{ns} {}
	JoinedSelectorMock(std::string_view ns) : ns_{ns} {}
	std::string_view RightNsName() const { return ns_; }

private:
	std::string_view ns_;
};
