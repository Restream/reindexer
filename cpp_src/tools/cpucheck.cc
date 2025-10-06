#if REINDEXER_WITH_SSE

#include "cpucheck.h"
#include "tools/errors.h"

#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
#ifdef _MSC_VER
#include <intrin.h>
static void cpuid_count(uint32_t out[4], uint32_t eax, uint32_t ecx) {
	int32_t _out[4];
	__cpuidex(_out, eax, ecx);
	std::memcpy(out, _out, sizeof(_out));
}
static __int64 xgetbv(uint32_t x) { return _xgetbv(x); }
#else  // _MSC_VER
#include <cpuid.h>
#include <stdint.h>
#include <x86intrin.h>
static void cpuid_count(uint32_t cpuInfo[4], uint32_t eax, uint32_t ecx) {
	__cpuid_count(eax, ecx, cpuInfo[0], cpuInfo[1], cpuInfo[2], cpuInfo[3]);
}
static uint64_t xgetbv(uint32_t index) {
	uint32_t eax, edx;
	__asm__ __volatile__("xgetbv" : "=a"(eax), "=d"(edx) : "c"(index));
	return (uint64_t(edx) << 32) | eax;
}
#endif	// !_MSC_VER
#endif	// defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)

#define _XCR_XFEATURE_ENABLED_MASK 0

namespace reindexer {

class [[nodiscard]] CPUInfo {
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

	static void cpuid(uint32_t* cpuinfo, uint32_t info) noexcept {
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
#ifdef _MSC_VER
		int32_t _cpuinfo[4];
		__cpuid(_cpuinfo, int32_t(info));
		std::memcpy(cpuinfo, _cpuinfo, sizeof(_cpuinfo));
#else	// _MSC_VER
		__cpuid(info, cpuinfo[0], cpuinfo[1], cpuinfo[2], cpuinfo[3]);
#endif	// _MSC_VER
#else	// defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
		(void)info;
		(void)cpuinfo;
#endif	// defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
	}

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

// Implementations from https://github.com/nmslib/hnswlib
// Not using CPUInfo-singletone here, because multiple different cpuid calls required
static bool cpuHasAVX() noexcept {
	uint32_t cpuInfo[4];

	// CPU support
	cpuid_count(cpuInfo, 0, 0);
	uint32_t nIds = cpuInfo[0];

	bool HW_AVX = false;
	if (nIds >= 0x00000001) {
		cpuid_count(cpuInfo, 0x00000001, 0);
		HW_AVX = (cpuInfo[2] & (uint32_t(1) << 28)) != 0;
	}

	// OS support
	cpuid_count(cpuInfo, 1, 0);

	bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
	bool cpuAVXSuport = (cpuInfo[2] & (uint32_t(1) << 28)) != 0;

	bool avxSupported = false;
	if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
		uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
		avxSupported = (xcrFeatureMask & 0x6) == 0x6;
	}
	return HW_AVX && avxSupported;
}
static bool cpuHasAVX2() noexcept {
	if (!CPUHasAVX()) {
		return false;
	}

	uint32_t cpuInfo[4];

	// CPU support
	cpuid_count(cpuInfo, 0, 0);
	uint32_t nIds = cpuInfo[0];

	bool HW_AVX2 = false;
	if (nIds >= 0x00000007) {  //  AVX2 Foundation
		cpuid_count(cpuInfo, 0x00000007, 0);
		HW_AVX2 = (cpuInfo[1] & (uint32_t(1) << 5)) != 0;
	}

	// OS support
	cpuid_count(cpuInfo, 1, 0);

	bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
	bool cpuAVXSuport = (cpuInfo[2] & (1 << 28)) != 0;

	bool avx2Supported = false;
	if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
		uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
		avx2Supported = (xcrFeatureMask & 0x06) == 0x06;
	}
	return HW_AVX2 && avx2Supported;
}
static bool cpuHasAVX512() noexcept {
	if (!CPUHasAVX()) {
		return false;
	}

	uint32_t cpuInfo[4];

	// CPU support
	cpuid_count(cpuInfo, 0, 0);
	uint32_t nIds = cpuInfo[0];

	bool HW_AVX512F = false;
	if (nIds >= 0x00000007) {  //  AVX512 Foundation
		cpuid_count(cpuInfo, 0x00000007, 0);
		HW_AVX512F = (cpuInfo[1] & (uint32_t(1) << 16)) != 0;
	}

	// OS support
	cpuid_count(cpuInfo, 1, 0);

	bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
	bool cpuAVXSuport = (cpuInfo[2] & (1 << 28)) != 0;

	bool avx512Supported = false;
	if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
		uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
		avx512Supported = (xcrFeatureMask & 0xe6) == 0xe6;
	}
	return HW_AVX512F && avx512Supported;
}

bool CPUHasAVX() noexcept {
	const static bool CPUHasAVXF = cpuHasAVX();
	return CPUHasAVXF;
}
bool CPUHasAVX2() noexcept {
	const static bool CPUHasAVX2F = cpuHasAVX2();
	return CPUHasAVX2F;
}
bool CPUHasAVX512() noexcept {
	const static bool CPUHasAVX512F = cpuHasAVX512();
	return CPUHasAVX512F;
}

void CheckRequiredSSESupport() {
	if (!reindexer::CPUHasFullSSE()) {
		throw Error(errSystem,
					"Current CPU does not support all the required SSE versions (up to 4.2) and POPCNT instruction. Unable to run current "
					"build of the reindexer. To fix this, rebuild reindexer with 'ENABLE_SSE=OFF' in CMake arguments");
	}
}

enum class [[nodiscard]] InstructionType { SSE, AVX, AVX2, AVX512f };

static InstructionType initAllowedInstructionsType() noexcept {
	static const auto kTargetInstructionsPtr = std::getenv("RX_TARGET_INSTRUCTIONS");
	static const std::string kTargetInstructions = kTargetInstructionsPtr ? kTargetInstructionsPtr : std::string();
	const bool allowAVX512 = kTargetInstructions.empty() || kTargetInstructions == "avx512";
	if (allowAVX512 && CPUHasAVX512()) {
		return InstructionType::AVX512f;
	}
	const bool allowAVX2 = allowAVX512 || kTargetInstructions == "avx2";
	if (allowAVX2 && CPUHasAVX2()) {
		return InstructionType::AVX2;
	}
	const bool allowAVX = allowAVX2 || kTargetInstructions == "avx";
	if (allowAVX && CPUHasAVX()) {
		return InstructionType::AVX;
	}
	return InstructionType::SSE;
}

bool IsAVX512Allowed() noexcept {
	static const InstructionType instructionsType = initAllowedInstructionsType();
	return (instructionsType == InstructionType::AVX512f);
}
bool IsAVX2Allowed() noexcept {
	static const InstructionType instructionsType = initAllowedInstructionsType();
	return IsAVX512Allowed() || (instructionsType == InstructionType::AVX2);
}
bool IsAVXAllowed() noexcept {
	static const InstructionType instructionsType = initAllowedInstructionsType();
	return IsAVX2Allowed() || (instructionsType == InstructionType::AVX);
}

}  // namespace reindexer

#else  // REINDEXER_WITH_SSE

namespace reindexer {
void CheckRequiredSSESupport() {
	// Empty. No SSE support required
}

bool CPUHasAVX() noexcept { return false; }
bool CPUHasAVX2() noexcept { return false; }
bool CPUHasAVX512() noexcept { return false; }
bool IsAVX512Allowed() noexcept { return false; }
bool IsAVX2Allowed() noexcept { return false; }
bool IsAVXAllowed() noexcept { return false; }

}  // namespace reindexer

#endif	// REINDEXER_WITH_SSE
