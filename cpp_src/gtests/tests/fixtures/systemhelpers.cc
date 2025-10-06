#include "systemhelpers.h"
#include <unistd.h>
#include <iostream>
#include <thread>
#include "tools/errors.h"

#ifndef _WIN32
#include <sys/wait.h>
#endif

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#ifdef __linux__
#include <sys/prctl.h>
#endif

namespace reindexer {

static const std::thread::id kMainThreadID = std::this_thread::get_id();

pid_t StartProcess(const std::string& program, const std::vector<std::string>& params) {
#ifdef __linux__
	std::vector<char*> paramsPointers;
	paramsPointers.reserve(params.size());
	for (size_t i = 0; i < params.size(); i++) {
		paramsPointers.push_back(const_cast<char*>(params[i].c_str()));
	}
	paramsPointers.push_back(nullptr);
	pid_t ppid_before_fork = getpid();
	const bool isMainThread = kMainThreadID == std::this_thread::get_id();
	pid_t processPID = fork();
	if (processPID == 0) {
		if (isMainThread) {	 // prctl sends signal on thread termination, so this call may lead to unexpected process termination
			int r = prctl(PR_SET_PDEATHSIG, SIGTERM);
			if (r == -1) {
				perror("prctl error");
				exit(1);
			}
		}
		if (getppid() != ppid_before_fork) {
			fprintf(stderr, "reindexer error: parent process is dead\n");
			exit(1);
		}
		int ret = execv(program.c_str(), &paramsPointers[0]);
		if (ret) {
			perror("exec error");
			exit(1);
		}
	}
	return processPID;
#else
	(void)program;
	(void)params;
	assertrx(false);
#endif
	return 0;
}

Error EndProcess(pid_t PID) {
#ifdef __linux__
	int r = kill(PID, SIGTERM);
	if (r != 0) {
		return Error(errLogic, "errno={} ({})", errno, strerror(errno));
	}
#else
	(void)PID;
	assertrx(false);
#endif
	return errOK;
}

Error WaitEndProcess(pid_t PID) {
#ifdef __linux__
	int status = 0;
	pid_t waitres = waitpid(PID, &status, 0);
	if (!WIFEXITED(status)) {
		return Error(errLogic, "WIFEXITED(status): false. status: {}", status);
	}
	if (WEXITSTATUS(status)) {
		return Error(errLogic, "WEXITSTATUS(status) != 0. status: {}", WEXITSTATUS(status));
	}
	if (waitres != PID) {
		return Error(errLogic, "waitres != PID. errno={} ({})", errno, strerror(errno));
	}
#else
	(void)PID;
	assertrx(false);
#endif
	return errOK;
}

}  // namespace reindexer
