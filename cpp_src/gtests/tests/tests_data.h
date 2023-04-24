#pragma once
#include <string_view>

#ifndef REINDEXER_TESTS_DATA_PATH
#define REINDEXER_TESTS_DATA_PATH ""
#endif

constexpr std::string_view kTestsDataPath(REINDEXER_TESTS_DATA_PATH);
