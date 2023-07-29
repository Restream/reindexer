#if REINDEXER_WITH_SSE

#include "cpucheck.h"
#include "tools/errors.h"

#ifdef __GNUC__
#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#endif	// defined(__x86_64__) || defined(__i386__)
#endif	// __GNUC__

namespace reindexer {

class CPUInfo {
public:
	static CPUInfo& Init() noexcept {
		thread_local CPUInfo info;
		return info;
	}
	bool HasSSE() const noexcept { return data_[3] & (1 << 25); }
	bool HasSSE2() const noexcept { return data_[3] & (1 << 26); }
	bool HasSSE3() const noexcept { return data_[2] & (1 << 0); }
	bool HasSSSE3() const noexcept { return data_[2] & (1 << 9); }
	bool HasPOPCNT() const noexcept { return data_[2] & (1 << 23); }
	bool HasSSE41() const noexcept { return data_[2] & (1 << 19); }
	bool HasSSE42() const noexcept { return data_[2] & (1 << 20); }

private:
	CPUInfo() {
		std::memset(data_, 0, sizeof(data_));
		cpuid(data_, 1);
	}

#ifdef __GNUC__
	static void cpuid(uint32_t* cpuinfo, uint32_t info) noexcept {
#if defined(__x86_64__) || defined(__i386__)
		__cpuid(info, cpuinfo[0], cpuinfo[1], cpuinfo[2], cpuinfo[3]);
#else	// defined(__x86_64__) || defined(__i386__)
		(void)info;
		(void)cpuinfo;
#endif	// defined(__x86_64__) || defined(__i386__)
	}
#endif	// __GNUC__

	uint32_t data_[4];
};

static bool CPUHasSSE() noexcept { return CPUInfo::Init().HasSSE(); }
static bool CPUHasSSE2() noexcept { return CPUInfo::Init().HasSSE2(); }
static bool CPUHasSSE3() noexcept { return CPUInfo::Init().HasSSE3(); }
static bool CPUHasSSSE3() noexcept { return CPUInfo::Init().HasSSSE3(); }
static bool HasPOPCNT() noexcept { return CPUInfo::Init().HasPOPCNT(); }
static bool CPUHasSSE41() noexcept { return CPUInfo::Init().HasSSE41(); }
static bool CPUHasSSE42() noexcept { return CPUInfo::Init().HasSSE42(); }
static bool CPUHasFullSSE() noexcept {
	thread_local const bool hasAllSSE =
		CPUHasSSE() && CPUHasSSE2() && CPUHasSSE3() && CPUHasSSSE3() && CPUHasSSE41() && CPUHasSSE42() && HasPOPCNT();
	return hasAllSSE;
}

void CheckRequiredSSESupport() {
	if (!reindexer::CPUHasFullSSE()) {
		throw Error(errSystem,
					"Current CPU does not support all the required SSE versions (up to 4.2) and POPCNT instruction. Unable to run current "
					"build of the reindexer. To fix this, rebuild reindexer with 'ENABLE_SSE=OFF' in CMake arguments");
	}
}

}  // namespace reindexer

#else  // REINDEXER_WITH_SSE

namespace reindexer {
void CheckRequiredSSESupport() {
	// Empty. No SSE support required
}
}  // namespace reindexer

#endif	// REINDEXER_WITH_SSE
