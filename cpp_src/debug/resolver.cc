
#include <iomanip>
#include <iostream>

#if REINDEX_WITH_LIBDL
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <cxxabi.h>
#include <dlfcn.h>
#include <unistd.h>
#endif

#ifdef __linux__
#define REINDEX_WITH_LIBBACKTRACE 1
#endif

#if REINDEX_WITH_LIBBACKTRACE
#include "libbacktrace/config.h"
// config.h must be included before another headers
#include "libbacktrace/backtrace.h"
#include "libbacktrace/internal.h"
#elif REINDEX_WITH_APPLE_SYMBOLICATION
#include <mach/mach.h>
#endif

// There are 3 symbol resolver methods available:
// 1. libdl - base symbolizer to provide symbolic names from addresses by dladdr call
// 2. libbacktrace - get extra info about source file + line location
// 3. Apple symbolication private framework - get extra info about source file + line location

#include "resolver.h"
#include "tools/fsops.h"

namespace reindexer {
namespace debug {

using namespace std::string_view_literals;

TraceEntry::TraceEntry(TraceEntry&& other) noexcept
	: funcName_(other.funcName_),
	  objFile_(other.objFile_),
	  srcFile_(other.srcFile_),
	  srcLine_(other.srcLine_),
	  ofs_(other.ofs_),
	  addr_(other.addr_),
	  baseAddr_(other.baseAddr_),
	  holder_(other.holder_) {
	other.holder_ = nullptr;
}

TraceEntry& TraceEntry::operator=(TraceEntry&& other) noexcept {
	if (this != &other) {
		funcName_ = other.funcName_;
		objFile_ = other.objFile_;
		srcFile_ = other.srcFile_;
		srcLine_ = other.srcLine_;
		ofs_ = other.ofs_;
		addr_ = other.addr_;
		baseAddr_ = other.baseAddr_;
		holder_ = other.holder_;
		other.holder_ = nullptr;
	}
	return *this;
}

TraceEntry::~TraceEntry() {
	free(holder_);
	holder_ = nullptr;
}

TraceEntry::TraceEntry(uintptr_t addr) {
	addr_ = addr;
#if REINDEX_WITH_LIBDL
	Dl_info dl_info;

	if (!dladdr(reinterpret_cast<void*>(addr), &dl_info)) {
		return;
	}

	objFile_ = dl_info.dli_fname;
	ofs_ = uintptr_t(addr) - uintptr_t(dl_info.dli_saddr);
	baseAddr_ = uintptr_t(dl_info.dli_fbase);

	int status;
	if ((holder_ = abi::__cxa_demangle(dl_info.dli_sname, NULL, NULL, &status)) != 0) {
		funcName_ = holder_;
	} else if (dl_info.dli_sname) {
		funcName_ = dl_info.dli_sname;
	} else {
		funcName_ = "<unresolved_function>"sv;
	}
#endif
}

std::ostream& TraceEntry::Dump(std::ostream& os) const {
	os << "0x" << std::hex << std::setfill('0') << std::setw(14) << addr_ << " " << funcName_ << std::dec;
	if (srcLine_) {
		// pretty print file path:
		// - if source file not exists limit it's path by cpp_src dir
		// - if file is near subdir of current dir, then print relative path
		// - in other cases print absolute path

		std::string srcFile(srcFile_);
		if (fs::Stat(srcFile) != fs::StatFile) {
			auto pos = srcFile.find("cpp_src/");
			if (pos != std::string::npos) {
				srcFile = srcFile.substr(pos);
			}
		} else {
			srcFile = fs::GetRelativePath(srcFile, 2);
		}

		os << " (" << srcFile << ":" << srcLine_ << ")";
	} else {
		os << " + " << ofs_;
	}
	return os;
}

#if REINDEX_WITH_LIBBACKTRACE
class [[nodiscard]] TraceResolverLibbacktrace : public TraceResolver {
public:
	TraceResolverLibbacktrace() { init(); }

	bool Resolve(TraceEntry& te) override final {
		backtrace_pcinfo(state_, te.addr_, callback, errorCallback, &te);
		return true;
	}

protected:
	void init() { state_ = backtrace_create_state("/proc/self/exe", 1, errorCallback, NULL); }
	static void errorCallback(void* /*data*/, const char* msg, int errnum) {
		std::cerr << "libbacktarce error:" << msg << " " << errnum << std::endl;
	}

	static int callback(void* data, uintptr_t /*pc*/, const char* filename, int lineno, const char* /*function*/) {
		TraceEntry* te = reinterpret_cast<TraceEntry*>(data);
		if (filename) {
			te->srcFile_ = filename;
			te->srcLine_ = lineno;
		} else {
			te->srcFile_ = "<unresolver_filename>"sv;
			te->srcLine_ = 0;
		}
		return 0;
	}

protected:
	backtrace_state* state_ = nullptr;
};

std::unique_ptr<TraceResolver> TraceResolver::New() { return std::unique_ptr<TraceResolver>(new TraceResolverLibbacktrace()); }

#elif REINDEX_WITH_APPLE_SYMBOLICATION

class [[nodiscard]] TraceResolverApple : public TraceResolver {
public:
	TraceResolverApple() { init(); }
	~TraceResolverApple() { CSRelease(cs_); }

	bool Resolve(TraceEntry& te) {
		bool ret = false;
		if (!cs_.csCppData || !cs_.csCppObj) {
			return false;
		}

		auto info = CSSymbolicatorGetSourceInfoWithAddressAtTime(cs_, te.addr_, CS_NOW);
		auto sym = (info.csCppData && info.csCppObj) ? CSSourceInfoGetSymbol(info)
													 : CSSymbolicatorGetSymbolWithAddressAtTime(cs_, te.addr_, CS_NOW);

		if (!sym.csCppData || !sym.csCppObj) {
			return false;
		}

		auto owner = CSSymbolGetSymbolOwner(sym);
		if (owner.csCppData && owner.csCppObj) {
			if (info.csCppData && info.csCppObj) {
				te.srcFile_ = CSSourceInfoGetPath(info);
				te.srcLine_ = CSSourceInfoGetLineNumber(info);
			}
		}
		ret = true;

		return ret;
	}

protected:
	bool init() {
		auto hlib = dlopen("/System/Library/PrivateFrameworks/CoreSymbolication.framework/Versions/A/CoreSymbolication", RTLD_NOW);
		if (!hlib) {
			return false;
		}

		CSSymbolicatorCreateWithPid = reinterpret_cast<pCSSymbolicatorCreateWithPid>(dlsym(hlib, "CSSymbolicatorCreateWithPid"));
		CSRelease = reinterpret_cast<pCSRelease>(dlsym(hlib, "CSRelease"));
		CSSymbolicatorGetSymbolWithAddressAtTime =
			reinterpret_cast<pCSSymbolicatorGetSymbolWithAddressAtTime>(dlsym(hlib, "CSSymbolicatorGetSymbolWithAddressAtTime"));
		CSSymbolicatorGetSourceInfoWithAddressAtTime =
			reinterpret_cast<pCSSymbolicatorGetSourceInfoWithAddressAtTime>(dlsym(hlib, "CSSymbolicatorGetSourceInfoWithAddressAtTime"));
		CSSourceInfoGetLineNumber = reinterpret_cast<pCSSourceInfoGetLineNumber>(dlsym(hlib, "CSSourceInfoGetLineNumber"));
		CSSourceInfoGetPath = reinterpret_cast<pCSSourceInfoGetPath>(dlsym(hlib, "CSSourceInfoGetPath"));
		CSSourceInfoGetSymbol = reinterpret_cast<pCSSourceInfoGetSymbol>(dlsym(hlib, "CSSourceInfoGetSymbol"));
		CSSymbolGetSymbolOwner = reinterpret_cast<pCSSymbolGetSymbolOwner>(dlsym(hlib, "CSSymbolGetSymbolOwner"));

		bool ok = CSSymbolicatorCreateWithPid && CSRelease && CSSymbolicatorGetSymbolWithAddressAtTime &&
				  CSSymbolicatorGetSourceInfoWithAddressAtTime && CSSourceInfoGetLineNumber && CSSourceInfoGetPath &&
				  CSSourceInfoGetSymbol && CSSymbolGetSymbolOwner;
		if (ok) {
			cs_ = CSSymbolicatorCreateWithPid(getpid());
		}
		return ok;
	}

	struct [[nodiscard]] CSTypeRef {
		void* csCppData;
		void* csCppObj;
	};
	static uint64_t constexpr CS_NOW = 0x80000000;

	typedef CSTypeRef CSSymbolicatorRef;
	typedef CSTypeRef CSSourceInfoRef;
	typedef CSTypeRef CSSymbolOwnerRef;
	typedef CSTypeRef CSSymbolRef;

	typedef CSSymbolicatorRef (*pCSSymbolicatorCreateWithPid)(pid_t pid);
	typedef void (*pCSRelease)(CSTypeRef cs);
	typedef CSSymbolRef (*pCSSymbolicatorGetSymbolWithAddressAtTime)(CSSymbolicatorRef cs, vm_address_t addr, uint64_t time);
	typedef CSSourceInfoRef (*pCSSymbolicatorGetSourceInfoWithAddressAtTime)(CSSymbolicatorRef cs, vm_address_t addr, uint64_t time);
	typedef int (*pCSSourceInfoGetLineNumber)(CSSourceInfoRef info);
	typedef const char* (*pCSSourceInfoGetPath)(CSSourceInfoRef info);
	typedef CSSymbolRef (*pCSSourceInfoGetSymbol)(CSSourceInfoRef info);
	typedef CSSymbolOwnerRef (*pCSSymbolGetSymbolOwner)(CSSymbolRef sym);

	pCSSymbolicatorCreateWithPid CSSymbolicatorCreateWithPid;
	pCSRelease CSRelease;
	pCSSymbolicatorGetSymbolWithAddressAtTime CSSymbolicatorGetSymbolWithAddressAtTime;
	pCSSymbolicatorGetSourceInfoWithAddressAtTime CSSymbolicatorGetSourceInfoWithAddressAtTime;
	pCSSourceInfoGetLineNumber CSSourceInfoGetLineNumber;
	pCSSourceInfoGetPath CSSourceInfoGetPath;
	pCSSourceInfoGetSymbol CSSourceInfoGetSymbol;
	pCSSymbolGetSymbolOwner CSSymbolGetSymbolOwner;
	CSSymbolicatorRef cs_;
};

std::unique_ptr<TraceResolver> TraceResolver::New() { return std::unique_ptr<TraceResolver>(new TraceResolverApple()); }
#else

std::unique_ptr<TraceResolver> TraceResolver::New() { return std::unique_ptr<TraceResolver>(new TraceResolver()); }
#endif
}  // namespace debug
}  // namespace reindexer
