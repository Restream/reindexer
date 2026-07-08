#pragma once

#include "tools/errors.h"
#include "tools/openssl.h"

namespace reindexer {

enum class [[nodiscard]] HashAlgorithm { MD5, SHA256, SHA512 };

Error ParseCryptString(const std::string& input, std::string& outHash, std::string& outSalt, HashAlgorithm& hashAlgorithm);

std::string MD5crypt(const std::string& passwd, const std::string& salt) noexcept;
std::string shacrypt(const char* passwd, const char* magic, const char* salt);

namespace openssl {
bool LibCryptoAvailable();
}  // namespace openssl
}  // namespace reindexer

#define LIBCRYPTO_EXPAND_MACRO_LIST(MACRO) \
	MACRO(EVP_sha256)                      \
	MACRO(EVP_sha512)                      \
	MACRO(OPENSSL_strlcpy)                 \
	MACRO(OPENSSL_strlcat)                 \
	MACRO(EVP_MD_CTX_new)                  \
	MACRO(EVP_DigestInit_ex)               \
	MACRO(EVP_DigestUpdate)                \
	MACRO(EVP_DigestFinal_ex)              \
	MACRO(CRYPTO_zalloc)                   \
	MACRO(EVP_MD_CTX_free)                 \
	MACRO(CRYPTO_free)                     \
	MACRO(EVP_Digest)                      \
	MACRO(ERR_error_string)                \
	MACRO(ERR_get_error)

#define LIBCRYPTO_EXTERN_SYM(sym) extern decltype(::sym)* sym;

namespace reindexer::openssl {
#if WITH_OPENSSL
LIBCRYPTO_EXPAND_MACRO_LIST(LIBCRYPTO_EXTERN_SYM)
#else
LIBCRYPTO_EXPAND_MACRO_LIST(OPENSSL_DUMMY_SYM)
#endif

}  // namespace reindexer::openssl