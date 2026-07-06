#ifdef WITH_OPENSSL

#ifndef _WIN32
#include <dlfcn.h>
#else
#include <windows.h>

#include <errhandlingapi.h>
#include <libloaderapi.h>
#endif

#include <mutex>
#include "crypt.h"
#include "tls.h"
#include "tools/logger.h"

#if __linux__
#define LIBCRYPTO_NAME "libcrypto.so"
#define LIBSSL_NAME "libssl.so"
#elif __APPLE__
#define LIBCRYPTO_NAME "libcrypto.dylib"
#define LIBSSL_NAME "libssl.dylib"
#elif _WIN32
#define LIBCRYPTO_NAME "libcrypto-3-x64.dll"
#define LIBSSL_NAME "libssl-3-x64.dll"
#endif

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

namespace detail {
#define OPENSSL_INIT_SYM(sym)                                                                                            \
	, sym##_([this] {                                                                                                    \
		auto res = reinterpret_cast<decltype(::sym)*>(LOAD_SYM(lib_, #sym));                                             \
		if (!res) throw reindexer::Error(errNotFound, "Symbol '{}' was not found in OpenSSL lib: {}", #sym, LOAD_ERR()); \
		return res;                                                                                                      \
	}())

#define OPENSSL_DECL_SYM(sym) decltype(::sym)* sym##_ = nullptr;

#define OPENSSL_GET_SYM(sym) \
	static auto sym() { return instance().impl_.sym##_; }

#define OPENSSL_CREATE_CLASS(CLASS, LIB_NAME, EXPAND_MACRO)                                                             \
	class [[nodiscard]] CLASS {                                                                                         \
	public:                                                                                                             \
		static bool available() {                                                                                       \
			static auto& inst = instance();                                                                             \
			std::call_once(inst.logStatus_, []() {                                                                      \
				if (inst.status_.ok()) {                                                                                \
					logFmt(LogInfo, "The {} library with the required symbols has been successfully loaded", LIB_NAME); \
				} else {                                                                                                \
					reindexer::logPrint(LogError, inst.status_.what());                                                 \
				}                                                                                                       \
			});                                                                                                         \
			return inst.status_.ok();                                                                                   \
		}                                                                                                               \
                                                                                                                        \
		EXPAND_MACRO(OPENSSL_GET_SYM)                                                                                   \
                                                                                                                        \
	private:                                                                                                            \
		static CLASS& instance() noexcept {                                                                             \
			static CLASS inst;                                                                                          \
			return inst;                                                                                                \
		}                                                                                                               \
                                                                                                                        \
		CLASS() noexcept                                                                                                \
			: impl_([this]() {                                                                                          \
				  try {                                                                                                 \
					  return Impl();                                                                                    \
				  } catch (const reindexer::Error& err) {                                                               \
					  status_ = err;                                                                                    \
					  return Impl(nullptr);                                                                             \
				  }                                                                                                     \
			  }()) {}                                                                                                   \
                                                                                                                        \
		reindexer::Error status_;                                                                                       \
		struct [[nodiscard]] Impl {                                                                                     \
			static constexpr auto load = [](const char* name) {                                                         \
				auto res = ::LOAD_LIB(name);                                                                            \
				if (!res) {                                                                                             \
					throw reindexer::Error(errNotFound, "{} library could not be loaded: {}", name, LOAD_ERR());        \
				}                                                                                                       \
				return res;                                                                                             \
			};                                                                                                          \
                                                                                                                        \
			Impl(std::nullptr_t) noexcept {}                                                                            \
			Impl() : lib_(load(LIB_NAME)) EXPAND_MACRO(OPENSSL_INIT_SYM) {}                                             \
                                                                                                                        \
			LIB_TYPE lib_ = nullptr;                                                                                    \
			EXPAND_MACRO(OPENSSL_DECL_SYM)                                                                              \
		} impl_;                                                                                                        \
		std::once_flag logStatus_;                                                                                      \
	};

OPENSSL_CREATE_CLASS(_LibCryptoSingleton, LIBCRYPTO_NAME, LIBCRYPTO_EXPAND_MACRO_LIST)
OPENSSL_CREATE_CLASS(_LibSSLSingleton, LIBSSL_NAME, LIBSSL_EXPAND_MACRO_LIST)

}  // namespace detail

namespace reindexer::openssl {
#define LIBCRYPTO_DEF_SYM(sym) decltype(::sym)* sym = detail::_LibCryptoSingleton::sym();
#define LIBSSL_DEF_SYM(sym) decltype(::sym)* sym = detail::_LibSSLSingleton::sym();

LIBCRYPTO_EXPAND_MACRO_LIST(LIBCRYPTO_DEF_SYM)
LIBSSL_EXPAND_MACRO_LIST(LIBSSL_DEF_SYM)
}  // namespace reindexer::openssl

#endif

namespace reindexer::openssl {
bool LibCryptoAvailable() {
#ifdef WITH_OPENSSL
	return detail::_LibCryptoSingleton::available();
#else
	return false;
#endif
}

bool LibSSLAvailable() {
#ifdef WITH_OPENSSL
	return detail::_LibSSLSingleton::available();
#else
	return false;
#endif
}
}  // namespace reindexer::openssl
