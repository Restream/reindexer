#if RX_WITH_FAISS_ANN_INDEXES

#include <iostream>
#include "tools/errors.h"

#ifndef _WIN32
#include <dlfcn.h>
#else  // _WIN32
#include <windows.h>

#include <errhandlingapi.h>
#include <libloaderapi.h>
#endif	// _WIN32

/* declare BLAS function types */
typedef int (*T_sgemm_)(const char* transa, const char* transb, FINTEGER* m, FINTEGER* n, FINTEGER* k, const float* alpha, const float* a,
						FINTEGER* lda, const float* b, FINTEGER* ldb, float* beta, float* c, FINTEGER* ldc);

typedef int (*T_dgemm_)(const char* transa, const char* transb, FINTEGER* m, FINTEGER* n, FINTEGER* k, const double* alpha, const double* a,
						FINTEGER* lda, const double* b, FINTEGER* ldb, double* beta, double* c, FINTEGER* ldc);

typedef int (*T_ssyrk_)(const char* uplo, const char* trans, FINTEGER* n, FINTEGER* k, float* alpha, float* a, FINTEGER* lda, float* beta,
						float* c, FINTEGER* ldc);

typedef void (*T_sgetrf_)(FINTEGER* m, FINTEGER* n, float* a, FINTEGER* lda, FINTEGER* ipiv, FINTEGER* info);

typedef void (*T_sgetri_)(FINTEGER* n, float* a, FINTEGER* lda, FINTEGER* ipiv, float* work, FINTEGER* lwork, FINTEGER* info);

typedef void (*T_dgetri_)(FINTEGER* n, double* a, FINTEGER* lda, FINTEGER* ipiv, double* work, FINTEGER* lwork, FINTEGER* info);

typedef void (*T_dgetrf_)(FINTEGER* m, FINTEGER* n, double* a, FINTEGER* lda, FINTEGER* ipiv, FINTEGER* info);

/* declare Lapack function types */

typedef int (*T_ssyev_)(const char* jobz, const char* uplo, FINTEGER* n, float* a, FINTEGER* lda, float* w, float* work, FINTEGER* lwork,
						FINTEGER* info);

typedef int (*T_dsyev_)(const char* jobz, const char* uplo, FINTEGER* n, double* a, FINTEGER* lda, double* w, double* work, FINTEGER* lwork,
						FINTEGER* info);

typedef int (*T_sgesvd_)(const char* jobu, const char* jobvt, FINTEGER* m, FINTEGER* n, float* a, FINTEGER* lda, float* s, float* u,
						 FINTEGER* ldu, float* vt, FINTEGER* ldvt, float* work, FINTEGER* lwork, FINTEGER* info);

typedef int (*T_dgesvd_)(const char* jobu, const char* jobvt, FINTEGER* m, FINTEGER* n, double* a, FINTEGER* lda, double* s, double* u,
						 FINTEGER* ldu, double* vt, FINTEGER* ldvt, double* work, FINTEGER* lwork, FINTEGER* info);

typedef int (*T_sgelsd_)(FINTEGER* m, FINTEGER* n, FINTEGER* nrhs, float* a, FINTEGER* lda, float* b, FINTEGER* ldb, float* s, float* rcond,
						 FINTEGER* rank, float* work, FINTEGER* lwork, FINTEGER* iwork, FINTEGER* info);

typedef int (*T_sgeqrf_)(FINTEGER* m, FINTEGER* n, float* a, FINTEGER* lda, float* tau, float* work, FINTEGER* lwork, FINTEGER* info);

typedef int (*T_sorgqr_)(FINTEGER* m, FINTEGER* n, FINTEGER* k, float* a, FINTEGER* lda, float* tau, float* work, FINTEGER* lwork,
						 FINTEGER* info);

typedef int (*T_sgemv_)(const char* trans, FINTEGER* m, FINTEGER* n, float* alpha, const float* a, FINTEGER* lda, const float* x,
						FINTEGER* incx, float* beta, float* y, FINTEGER* incy);

#if defined(__linux__) || defined(__APPLE__)
#define LIB_TYPE void*
#define LOAD_LIB(libname) dlopen(libname, RTLD_NOW)
#define LOAD_SYM dlsym
#define LOAD_ERR dlerror
#elif _WIN32
#define LIB_TYPE HMODULE
#define LOAD_LIB(libname) LoadLibrary(libname)
#define LOAD_SYM GetProcAddress
#define LOAD_ERR GetLastError
#endif

namespace reindexer::blas_ext {

static LIB_TYPE findBLASPtr() {
// TODO: Probably we should support more implementations
#ifdef __APPLE__
	constexpr std::string_view kPossibleBlasNames[] = {"libopenblas-pthread.dylib", "libopenblas.dylib", "libblas.dylib",
													   "libflexiblas.dylib"};
#elif _WIN32
	constexpr std::string_view kPossibleBlasNames[] = {"libopenblas.dll", "openblas.dll",	  "libblas.dll",
													   "blas.dll",		  "libflexiblas.dll", "flexiblas.dll"};
#else
	constexpr std::string_view kPossibleBlasNames[] = {"libopenblas-pthread.so", "libopenblas.so", "libopenblasp.so", "libopenblasp64.so",
													   "libopenblas64.so",		 "libblas.so",	   "libflexiblas.so"};
#endif
	LIB_TYPE ptr = nullptr;
	const auto kEnvPtr = std::getenv("RX_CUSTOM_BLAS_LIB_NAME");
	if (kEnvPtr) {
		// NOLINTNEXTLINE (*GenericTaint)
		ptr = ::LOAD_LIB(kEnvPtr);
	} else {
		for (auto name : kPossibleBlasNames) {
			// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
			ptr = ::LOAD_LIB(name.data());
			if (ptr) {
				break;
			}
		}
	}
	return ptr;
}

static LIB_TYPE findLAPACKPtr() {
	// TODO: Probably we should support more implementations
#ifdef __APPLE__
	constexpr std::string_view kPossibleLapackNames[] = {"liblapack.dylib", "libflexiblas.dylib", "libopenblas-pthread.dylib",
														 "libopenblas.dylib"};
#elif _WIN32
	constexpr std::string_view kPossibleLapackNames[] = {"liblapack.dll", "lapack.dll",		  "libopenblas.dll",
														 "openblas.dll",  "libflexiblas.dll", "flexiblas.dll"};
#else
	constexpr std::string_view kPossibleLapackNames[] = {"liblapack.so",	"libflexiblas.so",	 "libopenblas-pthread.so", "libopenblas.so",
														 "libopenblasp.so", "libopenblasp64.so", "libopenblas64.so"};
#endif
	LIB_TYPE ptr = nullptr;
	const auto kEnvPtr = std::getenv("RX_CUSTOM_LAPACK_LIB_NAME");
	if (kEnvPtr) {
		// NOLINTNEXTLINE (*GenericTaint)
		ptr = ::LOAD_LIB(kEnvPtr);
	} else {
		for (auto name : kPossibleLapackNames) {
			// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
			ptr = ::LOAD_LIB(name.data());
			if (ptr) {
				break;
			}
		}
	}
	return ptr;
}

static LIB_TYPE getBLASPtr() {
	static LIB_TYPE ptr = findBLASPtr();
	return ptr;
}

static LIB_TYPE getLAPACKPtr() {
	static LIB_TYPE ptr = findLAPACKPtr();
	return ptr;
}

void checkIfBLASAvailable() {
	if (!getBLASPtr()) {
		throw Error(errLogic, "Unable to link BLAS/MKL library: {}", LOAD_ERR());
	}
}

void checkIfLAPACKAvailable() {
	if (!getLAPACKPtr()) {
		throw Error(errLogic, "Unable to link LAPACK library: {}", LOAD_ERR());
	}
}

void CheckIfBLASAvailable() {
	checkIfBLASAvailable();
	checkIfLAPACKAvailable();
}

#define define_try_get_blas_lapack_func(name)                                                                                     \
	auto get_##name() {                                                                                                           \
		try {                                                                                                                     \
			CheckIfBLASAvailable();                                                                                               \
		} catch (const Error& err) {                                                                                              \
			std::cerr << err.what() << std::endl;                                                                                 \
			std::abort(); /* No way to throw an exception from 'extern C' */                                                      \
		}                                                                                                                         \
		auto ptr = LOAD_SYM(getBLASPtr(), #name);                                                                                 \
		auto sym = reinterpret_cast<T_##name>(ptr);                                                                               \
		if (sym) {                                                                                                                \
			return sym;                                                                                                           \
		}                                                                                                                         \
		ptr = LOAD_SYM(getLAPACKPtr(), #name);                                                                                    \
		sym = reinterpret_cast<T_##name>(ptr);                                                                                    \
		std::cerr << fmt::format("Unable to find {}() function: {} neither in blas, nor lapack", #name, LOAD_ERR()) << std::endl; \
		std::abort(); /* No way to throw an exception from 'extern C' */                                                          \
		return sym;                                                                                                               \
	}

#ifdef __MINGW32__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-function-type"
#endif

define_try_get_blas_lapack_func(sgemm_);
define_try_get_blas_lapack_func(dgemm_);
define_try_get_blas_lapack_func(ssyrk_);
define_try_get_blas_lapack_func(sgetrf_);
define_try_get_blas_lapack_func(sgetri_);
define_try_get_blas_lapack_func(dgetri_);
define_try_get_blas_lapack_func(dgetrf_);

define_try_get_blas_lapack_func(ssyev_);
define_try_get_blas_lapack_func(dsyev_);
define_try_get_blas_lapack_func(sgesvd_);
define_try_get_blas_lapack_func(dgesvd_);
define_try_get_blas_lapack_func(sgelsd_);
define_try_get_blas_lapack_func(sgeqrf_);
define_try_get_blas_lapack_func(sorgqr_);
define_try_get_blas_lapack_func(sgemv_);

#ifdef __MINGW32__
#pragma GCC diagnostic pop
#endif

}  // namespace reindexer::blas_ext

extern "C" {
/* BLAS function wrappers */
int sgemm_dlwrp_(const char* transa, const char* transb, FINTEGER* m, FINTEGER* n, FINTEGER* k, const float* alpha, const float* a,
				 FINTEGER* lda, const float* b, FINTEGER* ldb, float* beta, float* c, FINTEGER* ldc) {
	static auto sgemm = reindexer::blas_ext::get_sgemm_();
	return sgemm(transa, transb, m, n, k, alpha, a, lda, b, ldb, beta, c, ldc);
}

int dgemm_dlwrp_(const char* transa, const char* transb, FINTEGER* m, FINTEGER* n, FINTEGER* k, const double* alpha, const double* a,
				 FINTEGER* lda, const double* b, FINTEGER* ldb, double* beta, double* c, FINTEGER* ldc) {
	static auto dgemm = reindexer::blas_ext::get_dgemm_();
	return dgemm(transa, transb, m, n, k, alpha, a, lda, b, ldb, beta, c, ldc);
}

int ssyrk_dlwrp_(const char* uplo, const char* trans, FINTEGER* n, FINTEGER* k, float* alpha, float* a, FINTEGER* lda, float* beta,
				 float* c, FINTEGER* ldc) {
	static auto ssyrk = reindexer::blas_ext::get_ssyrk_();
	return ssyrk(uplo, trans, n, k, alpha, a, lda, beta, c, ldc);
}

void sgetrf_dlwrp_(FINTEGER* m, FINTEGER* n, float* a, FINTEGER* lda, FINTEGER* ipiv, FINTEGER* info) {
	static auto sgetrf = reindexer::blas_ext::get_sgetrf_();
	sgetrf(m, n, a, lda, ipiv, info);
}

void sgetri_dlwrp_(FINTEGER* n, float* a, FINTEGER* lda, FINTEGER* ipiv, float* work, FINTEGER* lwork, FINTEGER* info) {
	static auto sgetri = reindexer::blas_ext::get_sgetri_();
	sgetri(n, a, lda, ipiv, work, lwork, info);
}

void dgetri_dlwrp_(FINTEGER* n, double* a, FINTEGER* lda, FINTEGER* ipiv, double* work, FINTEGER* lwork, FINTEGER* info) {
	static auto dgetri = reindexer::blas_ext::get_dgetri_();
	dgetri(n, a, lda, ipiv, work, lwork, info);
}

void dgetrf_dlwrp_(FINTEGER* m, FINTEGER* n, double* a, FINTEGER* lda, FINTEGER* ipiv, FINTEGER* info) {
	static auto dgetrf = reindexer::blas_ext::get_dgetrf_();
	dgetrf(m, n, a, lda, ipiv, info);
}

/* Lapack function wrappers */
int ssyev_dlwrp_(const char* jobz, const char* uplo, FINTEGER* n, float* a, FINTEGER* lda, float* w, float* work, FINTEGER* lwork,
				 FINTEGER* info) {
	static auto ssyev = reindexer::blas_ext::get_ssyev_();
	return ssyev(jobz, uplo, n, a, lda, w, work, lwork, info);
}

int dsyev_dlwrp_(const char* jobz, const char* uplo, FINTEGER* n, double* a, FINTEGER* lda, double* w, double* work, FINTEGER* lwork,
				 FINTEGER* info) {
	static auto dsyev = reindexer::blas_ext::get_dsyev_();
	return dsyev(jobz, uplo, n, a, lda, w, work, lwork, info);
}

int sgesvd_dlwrp_(const char* jobu, const char* jobvt, FINTEGER* m, FINTEGER* n, float* a, FINTEGER* lda, float* s, float* u, FINTEGER* ldu,
				  float* vt, FINTEGER* ldvt, float* work, FINTEGER* lwork, FINTEGER* info) {
	static auto sgesvd = reindexer::blas_ext::get_sgesvd_();
	return sgesvd(jobu, jobvt, m, n, a, lda, s, u, ldu, vt, ldvt, work, lwork, info);
}

int dgesvd_dlwrp_(const char* jobu, const char* jobvt, FINTEGER* m, FINTEGER* n, double* a, FINTEGER* lda, double* s, double* u,
				  FINTEGER* ldu, double* vt, FINTEGER* ldvt, double* work, FINTEGER* lwork, FINTEGER* info) {
	static auto dgesvd = reindexer::blas_ext::get_dgesvd_();
	return dgesvd(jobu, jobvt, m, n, a, lda, s, u, ldu, vt, ldvt, work, lwork, info);
}

int sgelsd_dlwrp_(FINTEGER* m, FINTEGER* n, FINTEGER* nrhs, float* a, FINTEGER* lda, float* b, FINTEGER* ldb, float* s, float* rcond,
				  FINTEGER* rank, float* work, FINTEGER* lwork, FINTEGER* iwork, FINTEGER* info) {
	static auto sgelsd = reindexer::blas_ext::get_sgelsd_();
	return sgelsd(m, n, nrhs, a, lda, b, ldb, s, rcond, rank, work, lwork, iwork, info);
}

int sgeqrf_dlwrp_(FINTEGER* m, FINTEGER* n, float* a, FINTEGER* lda, float* tau, float* work, FINTEGER* lwork, FINTEGER* info) {
	static auto sgeqrf = reindexer::blas_ext::get_sgeqrf_();
	return sgeqrf(m, n, a, lda, tau, work, lwork, info);
}

int sorgqr_dlwrp_(FINTEGER* m, FINTEGER* n, FINTEGER* k, float* a, FINTEGER* lda, float* tau, float* work, FINTEGER* lwork,
				  FINTEGER* info) {
	static auto sorgqr = reindexer::blas_ext::get_sorgqr_();
	return sorgqr(m, n, k, a, lda, tau, work, lwork, info);
}

int sgemv_dlwrp_(const char* trans, FINTEGER* m, FINTEGER* n, float* alpha, const float* a, FINTEGER* lda, const float* x, FINTEGER* incx,
				 float* beta, float* y, FINTEGER* incy) {
	static auto sgemv = reindexer::blas_ext::get_sgemv_();
	return sgemv(trans, m, n, alpha, a, lda, x, incx, beta, y, incy);
}
}

#endif	// RX_WITH_FAISS_ANN_INDEXES
