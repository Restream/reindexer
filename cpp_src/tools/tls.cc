#include "tls.h"
#include "net/ev/ev.h"
#include "tools/crypt.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer::openssl {
#ifdef WITH_OPENSSL

#define SSL_ERROR_LOG(param)                                           \
	Error err = Error(param, ERR_error_string(ERR_get_error(), NULL)); \
	logFmt(LogError, "{}", err.what());

#define SSL_ERROR_LOG_THROW(param) SSL_ERROR_LOG(param) throw err;

static void configure_context(SSL_CTX* ctx, const std::string& crtPath, const std::string& keyPath) {
	if (SSL_CTX_use_certificate_file(ctx, crtPath.c_str(), SSL_FILETYPE_PEM) <= 0) {
		SSL_ERROR_LOG(errNotFound)
		exit(EXIT_FAILURE);
	}

	if (SSL_CTX_use_PrivateKey_file(ctx, keyPath.c_str(), SSL_FILETYPE_PEM) <= 0) {
		SSL_ERROR_LOG(errNotFound)
		exit(EXIT_FAILURE);
	}
}

SslCtxPtr create_server_context(const std::string& crtPath, const std::string& keyPath) {
	const SSL_METHOD* method = TLS_server_method();
	if (!method) {
		SSL_ERROR_LOG_THROW(errNotValid)
	}

	SSL_CTX* ctx = SSL_CTX_new(method);
	if (!ctx) {
		SSL_ERROR_LOG(errNotValid)
		exit(EXIT_FAILURE);
	}

	configure_context(ctx, crtPath, keyPath);
	return std::make_unique<SslCtxPtr::element_type>(*ctx);
}

SslCtxPtr create_client_context() {
	const SSL_METHOD* method = SSLv23_method();
	if (!method) {
		SSL_ERROR_LOG_THROW(errNotValid)
	}

	SSL_CTX* ctx = SSL_CTX_new(method);
	if (!ctx) {
		SSL_ERROR_LOG_THROW(errNotValid)
	}
	return std::make_unique<SslCtxPtr::element_type>(*ctx);
}

SslPtr create_ssl(const SslCtxPtr& ctx) {
	SSL* ssl = SSL_new(*ctx);

	if (!ssl) {
		SSL_ERROR_LOG_THROW(errNotValid)
	}

	return std::make_unique<SslPtr::element_type>(*ssl);
}

#else
SslCtxPtr create_server_context(const std::string&, const std::string&) {
	throw Error(errLogic,
				"Invalid ssl function call. It is impossible to create a server context, Reindexer was built without TLS support");
	return nullptr;
}
SslCtxPtr create_client_context() {
	throw Error(errLogic,
				"Invalid ssl function call. It is impossible to create a client context, Reindexer was built without TLS support");
	return nullptr;
}

SslPtr create_ssl(const SslCtxPtr&) {
	throw Error(errLogic, "Invalid ssl function call. It is impossible to create an ssl object, Reindexer was built without TLS support");
	return nullptr;
}
#endif

template <auto SslMethod>
int ssl_handshake([[maybe_unused]] SslPtr& ssl) {
#if WITH_OPENSSL
	if (auto state = openssl::SSL_get_state(*ssl); state != openssl::OSSL_HANDSHAKE_STATE::TLS_ST_OK) {
		if (int res = (*SslMethod)(*ssl); res != 1) {
			int err = openssl::SSL_get_error(*ssl, res);
			switch (err) {
				case SSL_ERROR_WANT_READ:
					return net::ev::READ;
				case SSL_ERROR_WANT_WRITE:
					return net::ev::WRITE;
				default:
					perror(fmt::format("ssl handshake error (state = {}; err_code = {}; {})", int(state), err,
									   openssl::ERR_error_string(openssl::ERR_get_error(), NULL))
							   .data());
					return -err;
			}
		}
	}
#else
	assertrx(false);
#endif
	return 0;
}

template int ssl_handshake<&openssl::SSL_accept>(SslPtr& ssl);
template int ssl_handshake<&openssl::SSL_connect>(SslPtr& ssl);
}  // namespace reindexer::openssl