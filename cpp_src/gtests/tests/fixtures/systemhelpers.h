#pragma once
#include <string>
#include <vector>
#include "tools/errors.h"

namespace reindexer {

std::string GetExeFileName();
pid_t StartProcess(const std::string& program, const std::vector<std::string>& params);
Error EndProcess(pid_t PID);
Error WaitEndProcess(pid_t PID);

}  // namespace reindexer
