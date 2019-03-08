#pragma once
#include <stdio.h>

#define PRINTF(...)              \
	do {                         \
		printf("[   INFO   ] "); \
		printf(__VA_ARGS__);     \
	} while (0)

// C++ stream interface
class TestCout : public std::stringstream {
public:
    ~TestCout() {
        PRINTF("%s", str().c_str());
        fflush(stdout);
    }
};

#define TEST_COUT TestCout()
