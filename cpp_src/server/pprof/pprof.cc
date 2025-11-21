#include "pprof.h"
#include <stdlib.h>
#include <cstdlib>
#include <thread>
#include "debug/resolver.h"
#include "gperf_profiler.h"
#include "pprof/gperf_profiler.h"
#include "tools/alloc_ext/je_malloc_extension.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/fsops.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

#if REINDEX_WITH_GPERFTOOLS || REINDEX_WITH_JEMALLOC
static const std::string kProfileNamePrefix = "reindexer_server";
#endif	// REINDEX_WITH_GPERFTOOLS || REINDEX_WITH_JEMALLOC

namespace reindexer_server {
using namespace reindexer;

void Pprof::Attach(http::Router& router) {
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

int Pprof::Profile(http::Context& ctx) {
#if REINDEX_WITH_GPERFTOOLS
	long long seconds = 30;
	std::string_view secondsParam;
	std::string filePath = fs::JoinPath(fs::GetTempDir(), kProfileNamePrefix + ".profile");

	for (auto p : ctx.request->params) {
		if (p.name == "seconds") {
			secondsParam = p.val;
		}
	}

	if (secondsParam.length()) {
		seconds = stoi(secondsParam);
	}
	if (seconds < 1) {
		seconds = 30;
	}

	if (alloc_ext::TCMallocIsAvailable()) {
		std::ignore = pprof::ProfilerStart(filePath.c_str());
	}
	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	if (alloc_ext::TCMallocIsAvailable()) {
		pprof::ProfilerStop();
	}
	std::string content;
	if (fs::ReadFile(filePath, content) < 0) {
		return ctx.String(http::StatusNotFound, "Profile file not found");
	}
	return ctx.String(http::StatusOK, content);
#else
	return ctx.String(http::StatusInternalServerError, "Reindexer was compiled without gperftools. CPU profiling is not available");
#endif
}

int Pprof::ProfileHeap(http::Context& ctx) {
#if REINDEX_WITH_GPERFTOOLS
	if (pprof::GperfProfilerIsAvailable()) {
		if (std::getenv("HEAPPROFILE")) {
			char* profile = pprof::GetHeapProfile();
			int res = ctx.String(http::StatusOK, profile);
			free(profile);
			return res;
		}
		std::string profile;
		alloc_ext::instance()->GetHeapSample(&profile);
		return ctx.String(http::StatusOK, profile);
	} else {
		return ctx.String(http::StatusInternalServerError, "Reindexer was compiled with gperftools, but was not able to link it properly");
	}
#elif REINDEX_WITH_JEMALLOC
	std::string content;
	std::string filePath = fs::JoinPath(fs::GetTempDir(), kProfileNamePrefix + ".heapprofile");
	const char* pfp = &filePath[0];

	std::ignore = alloc_ext::mallctl("prof.dump", NULL, NULL, &pfp, sizeof(pfp));
	if (fs::ReadFile(filePath, content) < 0) {
		return ctx.String(http::StatusNotFound, "Profile file not found");
	}
	return ctx.String(http::StatusOK, content);

#else
	return ctx.String(http::StatusInternalServerError, "Reindexer was compiled without gperftools or jemalloc. Profiling is not available");
#endif
}

int Pprof::Growth(http::Context& ctx) {
#if REINDEX_WITH_GPERFTOOLS
	if (alloc_ext::TCMallocIsAvailable()) {
		std::string output;
		alloc_ext::instance()->GetHeapGrowthStacks(&output);
		return ctx.String(http::StatusOK, output);
	} else {
		return ctx.String(http::StatusInternalServerError, "Reindexer was compiled with gperftools, but was not able to link it properly");
	}
#else
	return ctx.String(http::StatusInternalServerError, "Reindexer was compiled without gperftools. Profiling is not available");
#endif
}

int Pprof::CmdLine(http::Context& ctx) { return ctx.String(http::StatusOK, "reindexer_server"); }

int Pprof::Symbol(http::Context& ctx) {
	using namespace std::string_view_literals;
	WrSerializer ser;

	std::string req;
	if (ctx.request->method == "POST") {
		req = ctx.body->Read(ctx.body->Pending());
		if (!req.length()) {
			req = urldecode2(ctx.request->params[0].name);
		}
	} else if (ctx.request->params.size()) {
		req = urldecode2(ctx.request->params[0].name);
	} else {
		ser << "num_symbols: 1\n"sv;
		return ctx.String(http::StatusOK, ser.DetachChunk());
	}

	char* endp;
	for (size_t pos = 0; pos != std::string::npos; pos = req.find_first_of(" +", pos)) {
		pos = req.find_first_not_of(" +", pos);
		if (pos == std::string::npos) {
			break;
		}
		uintptr_t addr = strtoull(&req[pos], &endp, 16);
		ser << std::string_view(&req[pos], endp - &req[pos]) << '\t';
		resolveSymbol(addr, ser);
		ser << '\n';
	}

	return ctx.String(http::StatusOK, ser.DetachChunk());
}

void Pprof::resolveSymbol(uintptr_t ptr, WrSerializer& out) {
	auto te = debug::TraceEntry(ptr);
	std::string_view symbol = te.FuncName();

	if (symbol.length() > 20 && symbol.substr(0, 3) == "_ZN") {
		out << symbol.substr(0, 20) << "...";
		return;
	}

	int tmpl = 0;
	for (unsigned p = 0; p < symbol.length();) {
		// strip out std:: and std::__1
		if (symbol.substr(p, 10) == "std::__1::") {
			p += 10;
		} else if (symbol.substr(p, 5) == "std::") {
			p += 5;
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
					if (tmpl == 0) {
						out << symbol[p];
					}
			}
			p++;
		}
	}
}
}  // namespace reindexer_server
