
#ifdef _WIN32

#include <malloc.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include "tools/assertrx.h"

#include "winservice.h"
namespace reindexer_server {

#define SERVER_STOP_WAIT 5000
#define SERVER_START_WAIT 5000

WinService* g_Service;
static std::string GetLastErrorAsString() {
	// Get the error message, if any.
	DWORD errorMessageID = ::GetLastError();
	if (errorMessageID == 0) {
		return std::string();  // No error message has been recorded
	}

	LPSTR messageBuffer = nullptr;
	size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
								 errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), LPSTR(&messageBuffer), 0, NULL);

	std::string message(messageBuffer, size);

	// Free the buffer.
	LocalFree(messageBuffer);

	return message;
}
WinService::WinService(const std::string& name, const std::string& displayName, std::function<void(void)> run,
					   std::function<void(void)> terminate, std::function<bool(void)> status)
	: name_(name), displayName_(displayName), run_(run), terminate_(terminate), status_(status) {
	g_Service = this;
	sshStatusHandle_ = NULL;
}

WinService::~WinService() { g_Service = 0; }

void WinService::Message(bool bError, const char* fmt, ...) {
	va_list va;
	va_start(va, fmt);
	char tempBuf[4096];
	vsprintf(tempBuf, fmt, va);
	MessageBox(NULL, tempBuf, displayName_.c_str(), MB_OK | (bError ? MB_ICONSTOP : MB_ICONINFORMATION));
	va_end(va);
}

static VOID WINAPI ServiceCtrl(DWORD dwCtrlCode) {
	assertrx(g_Service);
	g_Service->ServiceCtrl(dwCtrlCode);
}

static void WINAPI ServiceMain(DWORD dwArgc, LPTSTR lpszArgv[]) {
	assertrx(g_Service);
	g_Service->MainInternal(dwArgc, lpszArgv);
}

void WinService::ServiceCtrl(DWORD dwCtrlCode) {
	switch (dwCtrlCode) {
		case SERVICE_CONTROL_SHUTDOWN:
		case SERVICE_CONTROL_STOP:
			ReportStatusToSCMgr(SERVICE_STOP_PENDING, NO_ERROR, SERVER_STOP_WAIT);
			terminate_();
			while (status_()) {
				Sleep(1000);
				ReportStatusToSCMgr(SERVICE_STOP_PENDING, NO_ERROR, SERVER_STOP_WAIT);
			}
			ReportStatusToSCMgr(SERVICE_STOPPED, 0, 0);
			break;
		default:
			ReportStatusToSCMgr(ssStatus_.dwCurrentState, NO_ERROR, 0);
	}
}

void WinService::MainInternal(DWORD dwArgc, LPTSTR lpszArgv[]) {
	(void)dwArgc;
	(void)lpszArgv;
	if ((sshStatusHandle_ = RegisterServiceCtrlHandler(name_.c_str(), reindexer_server::ServiceCtrl)) != NULL) {
		memset(&ssStatus_, 0, sizeof(ssStatus_));

		ssStatus_.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
		ssStatus_.dwServiceSpecificExitCode = 0;

		if (ReportStatusToSCMgr(SERVICE_START_PENDING, NO_ERROR, SERVER_START_WAIT)) {
			ReportStatusToSCMgr(SERVICE_RUNNING, NO_ERROR, 0);
			run_();
		}
		ReportStatusToSCMgr(SERVICE_STOPPED, 0, 0);
	}
}

bool WinService::ReportStatusToSCMgr(DWORD dwCurrentState, DWORD dwWin32ExitCode, DWORD dwWaitHint) {
	static DWORD dwCheckPoint = 1;

	if (dwCurrentState == SERVICE_START_PENDING) {
		ssStatus_.dwControlsAccepted = 0;
	} else {
		ssStatus_.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
	}

	ssStatus_.dwCurrentState = dwCurrentState;
	ssStatus_.dwWin32ExitCode = dwWin32ExitCode;
	ssStatus_.dwWaitHint = dwWaitHint;

	if ((dwCurrentState == SERVICE_RUNNING) || (dwCurrentState == SERVICE_STOPPED)) {
		ssStatus_.dwCheckPoint = 0;
	} else {
		ssStatus_.dwCheckPoint = dwCheckPoint++;
	}

	return SetServiceStatus(sshStatusHandle_, &ssStatus_);
}

bool WinService::Install(const char* cmdline) {
	SC_HANDLE schService = NULL, schSCManager = NULL;

	Remove(true);
	if ((schSCManager = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS)) != NULL) {
		schService = CreateService(schSCManager,			   // SCManager database
								   name_.c_str(),			   // name of service
								   displayName_.c_str(),	   // name to display
								   SERVICE_ALL_ACCESS,		   // desired access
								   SERVICE_WIN32_OWN_PROCESS,  // service type
								   SERVICE_AUTO_START,		   // start type
								   SERVICE_ERROR_NORMAL,	   // error control type
								   cmdline,					   // service's binary
								   NULL,					   // no load ordering group
								   NULL,					   // no tag identifier
								   NULL,					   // dependencies
								   NULL,					   // LocalSystem account
								   NULL);					   // no password

		if (schService != NULL) {
			CloseServiceHandle(schService);
			CloseServiceHandle(schSCManager);

			return true;
		} else {
			Message(false, "CreateService failed:\n{}\n", GetLastErrorAsString().c_str());
		}

		CloseServiceHandle(schSCManager);
	} else {
		Message(true, "OpenSCManager failed:\n{}\n", GetLastErrorAsString().c_str());
	}
	return false;
}

int WinService::Start() {
	SERVICE_TABLE_ENTRY DispTable[] = {{const_cast<char*>(name_.c_str()), ServiceMain}, {NULL, NULL}};
	StartServiceCtrlDispatcher(DispTable);
	return 0;
}

bool WinService::Remove(bool silent) {
	SC_HANDLE schService = NULL, schSCManager = NULL;

	if ((schSCManager = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS)) != NULL) {
		schService = OpenService(schSCManager, name_.c_str(), SERVICE_ALL_ACCESS);

		if (schService != NULL) {
			if (ControlService(schService, SERVICE_CONTROL_STOP, &ssStatus_)) {
				Sleep(1000);

				while (QueryServiceStatus(schService, &ssStatus_)) {
					if (ssStatus_.dwCurrentState == SERVICE_STOP_PENDING) {
						Sleep(1000);
					} else {
						break;
					}
				}
			}

			if (DeleteService(schService)) {
				CloseServiceHandle(schService);
				CloseServiceHandle(schSCManager);

				return true;
			} else if (!silent) {
				Message(true, "DeleteService failed:\n{}\n", GetLastErrorAsString().c_str());
			}

			CloseServiceHandle(schService);
		} else if (!silent) {
			Message(true, "OpenService failed:\n{}\n", GetLastErrorAsString().c_str());
		}

		CloseServiceHandle(schSCManager);
	} else if (!silent) {
		Message(true, "OpenSCManager failed:\n{}\n", GetLastErrorAsString().c_str());
	}

	return false;
}

}  // namespace reindexer_server

#else
// suppress clang warning
int ___winservice_dummy_suppress_warning;
#endif
