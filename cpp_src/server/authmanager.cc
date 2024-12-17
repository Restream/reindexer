#include "dbmanager.h"

#ifndef WITH_OPENSSL
#define SHA256_DIGEST_LENGTH 32
#endif

namespace reindexer_server {

void AuthManager::Init(const DBManager& dbmanager) {
	if (!openssl::LibCryptoAvailable()) {
		return;
	}

	for (const auto& [login, _] : dbmanager.users_) {
		userAuthData_[login];
	}
}

bool AuthManager::Check(const std::string& user, const std::string& passwd) const {
	if (!openssl::LibCryptoAvailable()) {
		return false;
	}

	auto it = userAuthData_.find(user);
	if (it == userAuthData_.end()) {
		return false;
	}

	unsigned char hash[SHA256_DIGEST_LENGTH];
	std::shared_lock lk(mtx_);
	return it->second.check(Token::generate(passwd, hash));
}

void AuthManager::Refresh(const std::string& user, const std::string& passwd) {
	if (!openssl::LibCryptoAvailable()) {
		return;
	}

	auto it = userAuthData_.find(user);
	if (it == userAuthData_.end()) {
		return;
	}

	unsigned char hash[SHA256_DIGEST_LENGTH];
	std::unique_lock lk(mtx_);
	it->second.refresh(Token::generate(passwd, hash));
}

std::string_view AuthManager::Token::generate(const std::string& passwd, unsigned char* hash) {
#if WITH_OPENSSL
	openssl::EVP_Digest(passwd.data(), passwd.size(), hash, nullptr, openssl::EVP_sha256(), nullptr);
	return {reinterpret_cast<char*>(hash), SHA256_DIGEST_LENGTH};
#else
	(void)passwd;
	(void)hash;
	return {};
#endif
}

bool AuthManager::Token::check(std::string_view token) const noexcept {
	return token == token_ && std::chrono::system_clock::now() - lastUpd_ < std::chrono::hours(1);
}

void AuthManager::Token::refresh(std::string_view token) {
	if (token_.empty()) {
		token_ = token;
	}

	if (token != token_) {
		throw Error(errParams, "Incorrect auth-token for refresh");
	}

	lastUpd_ = std::chrono::system_clock::now();
}

}  // namespace reindexer_server