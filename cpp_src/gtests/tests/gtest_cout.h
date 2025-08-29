#pragma once
#include <stdio.h>
#include <sstream>

#define PRINTF(...)              \
	do {                         \
		printf("[   INFO   ] "); \
		printf(__VA_ARGS__);     \
	} while (0)

/** @brief C++ Gtest log stream interface.
 * If you need your strings to be displayed in Tests execution log then they shall always end with "<<std::endl;" */
class [[nodiscard]] TestCout : public std::stringstream {
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

/** @brief GTest log stream helper. If you need your strings to be displayed in Tests execution log then they shall always end with
 * "<<std::endl;" */
#define TEST_COUT TestCout()
