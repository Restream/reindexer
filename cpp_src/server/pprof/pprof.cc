#include "pprof.h"
#include <stdlib.h>
#include <cstdlib>
#include <thread>
#include "debug/backtrace.h"
#include "estl/chunk_buf.h"
#include "tools/fsops.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

#if REINDEX_WITH_GPERFTOOLS
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#else
static int ProfilerStart(const char *) { return 1; }
static void ProfilerStop(void) { return; }
#endif

#define NAME_PREFIX "reindexer_server"

namespace reindexer_server {
using namespace reindexer;

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
	router.POST<Pprof, &Pprof::Symbol>("/symbolz", this);
}

int Pprof::Profile(http::Context &ctx) {
	long long seconds = 30;
	string_view secondsParam;
	string filePath = fs::GetTempDir();

	for (auto p : ctx.request->params) {
		if (p.name == "seconds") secondsParam = p.val;
	}

	if (secondsParam.length()) seconds = stoi(secondsParam);
	if (seconds < 1) seconds = 30;

	filePath = fs::JoinPath(filePath, string(NAME_PREFIX) + ".profile");

	ProfilerStart(filePath.c_str());
	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	ProfilerStop();
	string content;
	if (fs::ReadFile(filePath, content) < 0) {
		return ctx.String(http::StatusNotFound, "File not found");
	}
	return ctx.String(http::StatusOK, content);
}

int Pprof::ProfileHeap(http::Context &ctx) {
#ifdef REINDEX_WITH_GPERFTOOLS
	if (std::getenv("HEAPPROFILE")) {
		char *profile = GetHeapProfile();
		int res = ctx.String(http::StatusOK, profile);
		free(profile);
		return res;
	}
	string profile;
	MallocExtension::instance()->GetHeapSample(&profile);
	return ctx.String(http::StatusOK, profile);

#endif
	return ctx.String(http::StatusInternalServerError, "Reindexer was compiled without gperftools. Profiling is not available");
}

int Pprof::Growth(http::Context &ctx) {
#ifdef REINDEX_WITH_GPERFTOOLS
	string output;
	MallocExtension::instance()->GetHeapGrowthStacks(&output);
	return ctx.String(http::StatusOK, output);
#else
	return ctx.String(http::StatusInternalServerError, "Reindexer was compiled without gperftools. Profiling is not available");
#endif
}

int Pprof::CmdLine(http::Context &ctx) { return ctx.String(http::StatusOK, "reindexer_server"); }

int Pprof::Symbol(http::Context &ctx) {
	WrSerializer ser;

#ifdef REINDEX_WITH_GPERFTOOLS
	string req;
	if (ctx.request->method == "POST") {
		req = ctx.body->Read(ctx.body->Pending());
		if (!req.length()) {
			req = urldecode2(ctx.request->params[0].name);
		}
	} else if (ctx.request->params.size()) {
		req = urldecode2(ctx.request->params[0].name);
	} else {
		ser << "num_symbols: 1\n"_sv;
		return ctx.String(http::StatusOK, ser.DetachChunk());
	}
	size_t pos = req.find_first_of(" +", 0);
	char *endp;
	for (; pos != string::npos; pos = req.find_first_of(" +", pos)) {
		pos = req.find_first_not_of(" +", pos);
		if (pos == string::npos) break;
		uintptr_t addr = strtoull(&req[pos], &endp, 16);
		char tmpBuf[128];
		snprintf(tmpBuf, sizeof(tmpBuf), "%p\t", reinterpret_cast<void *>(addr));
		ser << tmpBuf;
		resolveSymbol(addr, ser);
		ser << '\n';
	}
#else
	ser << "num_symbols: 0\n";
#endif

	return ctx.String(http::StatusOK, ser.DetachChunk());
}

void Pprof::resolveSymbol(uintptr_t ptr, WrSerializer &out) {
	char *csym = resolve_symbol(reinterpret_cast<void *>(ptr), true);
	//	string symbol = csym, out;
	string_view symbol = csym;

	if (symbol.length() > 20 && symbol.substr(0, 3) == "_ZN") {
		out << symbol.substr(0, 20) << "...";
		free(csym);
		return;
	}

	int tmpl = 0;
	for (unsigned p = 0; p < symbol.length(); p++) {
		// strip out std:: and std::__1
		if (symbol.substr(p, 10) == "std::__1::") {
			p += 10;
		} else if (symbol.substr(p, 4) == "std::") {
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
					if (tmpl == 0) out << symbol.substr(p, 1);
			}
		}
	}

	free(csym);
}
}  // namespace reindexer_server
