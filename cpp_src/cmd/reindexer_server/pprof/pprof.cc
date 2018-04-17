#include "pprof.h"
#include <stdlib.h>
#include <thread>
#include "debug/backtrace.h"
#include "tools/fsops.h"

#if REINDEX_WITH_GPERFTOOLS
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#else
static char *GetHeapProfile() { return nullptr; }
static int ProfilerStart(const char *) { return 1; }
static void ProfilerStop(void) { return; }
#endif

#define NAME_PREFIX "reindexer_server"

namespace reindexer_server {

void Pprof::Attach(http::Router &router) {
	router.GET<Pprof, &Pprof::Profile>("/debug/pprof/profile", this);
	router.GET<Pprof, &Pprof::Profile>("/pprof/profile", this);

	router.GET<Pprof, &Pprof::ProfileHeap>("/debug/pprof/heap", this);
	router.GET<Pprof, &Pprof::ProfileHeap>("/pprof/heap", this);

	router.GET<Pprof, &Pprof::Growth>("/debug/pprof/growth", this);
	router.GET<Pprof, &Pprof::Growth>("/pprof/growth", this);

	router.GET<Pprof, &Pprof::CmdLine>("/debug/pprof/cmdline", this);
	router.GET<Pprof, &Pprof::CmdLine>("/pprof/cmdline", this);

	router.GET<Pprof, &Pprof::Symbol>("/debug/pprof/symbol", this);
	router.POST<Pprof, &Pprof::Symbol>("/debug/pprof/symbol", this);
	router.GET<Pprof, &Pprof::Symbol>("/pprof/symbol", this);
	router.POST<Pprof, &Pprof::Symbol>("/pprof/symbol", this);
}

int Pprof::Profile(http::Context &ctx) {
	long long seconds = 30;
	const char *secondsParam = nullptr;
	string filePath;

	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "seconds")) secondsParam = p.val;
	}

	if (secondsParam != nullptr) seconds = atoll(secondsParam);
	if (seconds < 1) seconds = 30;

	if (const char *tmpDir = getenv("TMPDIR")) {
		if (tmpDir) filePath = tmpDir;
	} else {
		filePath = reindexer::GetCwd();
	}

	filePath = reindexer::JoinPath(filePath, string(NAME_PREFIX) + ".profile");

	ProfilerStart(filePath.c_str());
	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	ProfilerStop();
	string content;
	if (reindexer::ReadFile(filePath, content) < 0) {
		return ctx.String(http::StatusNotFound, "File not found");
	}
	return ctx.String(http::StatusOK, content);
}

int Pprof::ProfileHeap(http::Context &ctx) {
	char *profile = GetHeapProfile();
	int res = ctx.String(http::StatusOK, profile);
	free(profile);
	return res;
}

int Pprof::Growth(http::Context &ctx) {
#ifdef REINDEX_WITH_GPERFTOOLS
	string output;
	MallocExtension::instance()->GetHeapGrowthStacks(&output);
	return ctx.String(http::StatusOK, output);
#else
	return ctx.String(http::StatusNotFound, "Not found");
#endif
}

int Pprof::CmdLine(http::Context &ctx) { return ctx.String(http::StatusOK, "reindexer_server"); }

int Pprof::Symbol(http::Context &ctx) {
#ifdef REINDEX_WITH_GPERFTOOLS
	char *req = nullptr, *token = nullptr, *endp;
	if (!strcmp(ctx.request->method, "POST")) {
		req = reinterpret_cast<char *>(alloca(ctx.body->Pending() + 1));
		ssize_t nread = ctx.body->Read(req, ctx.body->Pending());
		req[nread] = 0;
		if (!nread) {
			req = const_cast<char *>(ctx.request->params[0].name);
		}
	} else if (ctx.request->params.size()) {
		req = const_cast<char *>(ctx.request->params[0].name);
	} else {
		ctx.writer->Write("num_symbols: 1\n");
	}

	while ((token = strtok_r(req, " +", &req))) {
		uintptr_t addr = strtoull(token, &endp, 16);
		char tmpBuf[2048];
		int n = snprintf(tmpBuf, sizeof(tmpBuf), "%p %s\n", reinterpret_cast<void *>(addr), resolveSymbol(addr).c_str());
		ctx.writer->Write(tmpBuf, n);
	}
#else
	ctx.writer->Write("num_symbols: 0\n");
#endif

	return 0;
}

string Pprof::resolveSymbol(uintptr_t ptr) {
	char *csym = resolve_symbol(reinterpret_cast<void *>(ptr), true);
	string symbol = csym, out;

	free(csym);

	if (symbol.length() > 20 && symbol.substr(0, 3) == "_ZN") {
		return symbol.substr(0, 20) + "...";
	}

	int tmpl = 0;
	for (unsigned p = 0; p < symbol.length(); p++) {
		// strip out std:: and std::__1
		if (symbol.substr(p, 10) == "std::__1::") {
			p += 9;
		} else if (symbol.substr(p, 4) == "std::__1::") {
			p += 4;
		} else {
			// strip out c++ templates args
			switch (symbol[p]) {
				case '<':
					tmpl++;
					break;
				case '>':
					tmpl--;
					break;
				default:
					if (tmpl == 0) out += symbol[p];
			}
		}
	}
	return out;
}
}  // namespace reindexer_server
