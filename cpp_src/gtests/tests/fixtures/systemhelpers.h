#pragma once
#include <string>
#include <vector>
#include "tools/errors.h"

namespace reindexer_tests {

std::string GetExeFileName();
pid_t StartProcess(const std::string& program, const std::vector<std::string>& params);
reindexer::Error EndProcess(pid_t PID);
reindexer::Error WaitEndProcess(pid_t PID);

}  // namespace reindexer_tests
