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
	void BoldOn() { printf("\e[1m"); }
	void BoldOff() { printf("\e[0m"); }
	TestCout& Endl() {
		if (str().size()) {
			PRINTF("%s", str().c_str());
			str(std::string());
		}
		printf("\n");
		fflush(stdout);
		return *this;
	}
	~TestCout() {
		if (str().size()) {
			PRINTF("%s", str().c_str());
		}
		fflush(stdout);
	}
};

#define TEST_COUT TestCout()
