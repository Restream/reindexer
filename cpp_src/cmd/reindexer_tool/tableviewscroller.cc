#include "tableviewscroller.h"
#include "client/coroqueryresults.h"
#include "core/queryresults/queryresults.h"
#include "iotools.h"
#include "tools/oscompat.h"

#include <stdio.h>
#include <iomanip>
#include <sstream>

namespace reindexer_tool {

int getStdInFD() {
#ifdef WIN32
	return _fileno(stdin);
#else
	return STDIN_FILENO;
#endif
}

bool checkForExit() {
	int ch = 0;
	size_t nread = ::read(getStdInFD(), &ch, 1);
	return ((nread <= 0) || (ch == '\n'));
}

void WaitEnterToContinue(std::ostream& o, int terminalWidth, const std::function<bool(void)>& isCanceled) {
	o << "\r--More--" << std::flush;
	while (!isCanceled()) {
#ifdef WIN32
		WSAEVENT wsaEvent = ::WSACreateEvent();
		SOCKET sock = ::WSASocket(AF_UNSPEC, 0, 0, NULL, 0, 0);
		::WSAEventSelect(sock, wsaEvent, FD_READ);
		if (checkForExit()) {
			break;
		}
#else
		fd_set read_fds;
		FD_ZERO(&read_fds);
		FD_SET(getStdInFD(), &read_fds);
		int ret = ::select(1, &read_fds, nullptr, nullptr, nullptr);
		if ((ret == -1) && (errno != EINTR)) {
			break;
		} else if ((ret != -1) && (errno != EINTR)) {
			if (FD_ISSET(getStdInFD(), &read_fds) && checkForExit()) {
				o << "\e[A\r";
				break;
			}
		}
#endif
	}
	o << "\r" << std::string(terminalWidth, ' ') << "\r" << std::flush;
	if (isCanceled()) {
		o << "Canceled" << std::endl;
	}
}

TableViewScroller::TableViewScroller(reindexer::TableViewBuilder& tableBuilder, int linesOnPage)
	: tableBuilder_(tableBuilder), linesOnPage_(linesOnPage) {}

void TableViewScroller::Scroll(Output& output, std::vector<std::string>&& jsonData, const std::function<bool(void)>& isCanceled) {
	if (isCanceled()) {
		return;
	}

	reindexer::TerminalSize terminalSize = reindexer::getTerminalSize();
	reindexer::TableCalculator tableCalculator(std::move(jsonData), terminalSize.width);
	auto& rows = tableCalculator.GetRows();
	bool viaMoreCmd = (int(rows.size()) > linesOnPage_);

#ifndef WIN32
	FILE* pfile = nullptr;
	if (viaMoreCmd) {
		pfile = popen("more", "w");
	}
#else
	viaMoreCmd = false;
#endif

	std::stringstream ss;
	std::ostream& o = viaMoreCmd ? ss : output();
	tableBuilder_.BuildHeader(o, tableCalculator, isCanceled);

	for (size_t i = 0; i < rows.size() && !isCanceled(); ++i) {
		tableBuilder_.BuildRow(o, i, tableCalculator);
#ifdef WIN32
		if ((i != 0) && (i % linesOnPage_ == 0)) {
			WaitEnterToContinue(o, terminalSize.width, isCanceled);
		}
#else
		if (viaMoreCmd && pfile) {
			std::string buf = ss.str();
			fwrite(buf.data(), sizeof(char), buf.length(), pfile);
			ss.str("");
		}
#endif
	}

#ifndef WIN32
	if (pfile) {
		pclose(pfile);
	}
#endif
}

}  // namespace reindexer_tool
