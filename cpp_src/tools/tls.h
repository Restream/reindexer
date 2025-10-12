#pragma once
#include <functional>
#include <memory>
#include <string>

#include "tools/openssl.h"

#define LIBSSL_EXPAND_MACRO_LIST(MACRO) \
	MACRO(SSL_CTX_use_certificate_file) \
	MACRO(SSL_CTX_use_PrivateKey_file)  \
	MACRO(TLS_server_method)            \
	MACRO(SSL_CTX_new)                  \
	MACRO(SSL_CTX_free)                 \
	MACRO(SSL_new)                      \
	MACRO(SSL_set_fd)                   \
	MACRO(SSL_get_fd)                   \
	MACRO(SSL_free)                     \
	MACRO(SSL_write)                    \
	MACRO(SSL_read)                     \
	MACRO(SSL_accept)                   \
	MACRO(SSL_get_error)                \
	MACRO(TLS_method)                   \
	MACRO(SSL_connect)                  \
	MACRO(SSL_get_state)

#ifdef WITH_OPENSSL

#define LIBSSL_EXTERN_SYM(sym) extern decltype(::sym)* sym;

namespace reindexer::openssl {

using OSSL_HANDSHAKE_STATE = ::OSSL_HANDSHAKE_STATE;

LIBSSL_EXPAND_MACRO_LIST(LIBSSL_EXTERN_SYM)
}  // namespace reindexer::openssl
#else

namespace reindexer::openssl {
struct [[nodiscard]] SSL_CTX {};
struct [[nodiscard]] SSL {};

typedef enum {
	TLS_ST_OK,
} OSSL_HANDSHAKE_STATE;

#define SSL_ERROR_WANT_READ 2
#define SSL_ERROR_WANT_WRITE 3
#define SSL_ERROR_SYSCALL 5

LIBSSL_EXPAND_MACRO_LIST(OPENSSL_DUMMY_SYM)
}  // namespace reindexer::openssl
#endif

namespace reindexer::openssl {

template <typename T, auto DeleterFn>
struct [[nodiscard]] SslWrapper {
	SslWrapper() = default;
	SslWrapper(T& obj) noexcept : obj(obj) {}
	operator T*() noexcept { return &obj; }
	operator const T*() const noexcept { return &obj; }

	~SslWrapper() { (*DeleterFn)(&obj); }
	T& obj;
};

using SslCtxPtr = std::unique_ptr<SslWrapper<SSL_CTX, &openssl::SSL_CTX_free>>;
using SslPtr = std::unique_ptr<SslWrapper<SSL, &openssl::SSL_free>>;

SslCtxPtr create_server_context(const std::string& crtPath, const std::string& keyPath);
SslCtxPtr create_client_context();
SslPtr create_ssl(const SslCtxPtr& ctx);

template <auto SslMethod>
int ssl_handshake(SslPtr& ssl);

bool LibSSLAvailable();
}  // namespace reindexer::openssl
