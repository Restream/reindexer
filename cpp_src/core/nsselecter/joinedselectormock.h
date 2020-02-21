#pragma once

#include "estl/string_view.h"

class JoinedSelectorMock {
public:
	JoinedSelectorMock(const char* ns) : ns_{ns} {}
	JoinedSelectorMock(reindexer::string_view ns) : ns_{ns} {}
	reindexer::string_view RightNsName() const { return ns_; }

private:
	reindexer::string_view ns_;
};
