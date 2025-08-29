#pragma once
#ifdef _WIN32

#include <functional>
#include <string>

#include "tools/oscompat.h"

#include <winsvc.h>

namespace reindexer_server {

class [[nodiscard]] WinService {
public:
	WinService(const std::string& name, const std::string& displayName, std::function<void(void)> run, std::function<void(void)> terminate,
			   std::function<bool(void)> status);
	virtual ~WinService();
	void Message(bool bError, const char* fmt, ...);
	int Start();
	bool Install(const char* cmdline);
	bool Remove(bool silent = false);

	void ServiceCtrl(DWORD dwCtrlCode);
	void MainInternal(DWORD argc, LPTSTR argv[]);

	bool ReportStatusToSCMgr(DWORD dwCurrentState, DWORD dwWin32ExitCode, DWORD dwWaitHint);

protected:
	SERVICE_STATUS ssStatus_;
	SERVICE_STATUS_HANDLE sshStatusHandle_;

	std::string name_, displayName_;
	std::function<void(void)> run_, terminate_;
	std::function<bool(void)> status_;
};

}  // namespace reindexer_server
#endif
