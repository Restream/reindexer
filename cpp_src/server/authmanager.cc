#include "dbmanager.h"
#include "estl/lock.h"

#ifndef WITH_OPENSSL
#define SHA256_DIGEST_LENGTH 32
#endif

namespace reindexer_server {

void AuthManager::Init(const DBManager& dbmanager) RX_NO_THREAD_SAFETY_ANALYSIS {
	if (!openssl::LibCryptoAvailable()) {
		return;
	}

	for (const auto& [login, _] : dbmanager.users_) {
		userAuthData_[login];
	}
}

auto AuthManager::findToken(const std::string& user) const& RX_NO_THREAD_SAFETY_ANALYSIS {
	return std::pair{userAuthData_.find(user), userAuthData_.end()};
}

auto AuthManager::findToken(const std::string& user) & RX_NO_THREAD_SAFETY_ANALYSIS {
	return std::pair{userAuthData_.find(user), userAuthData_.end()};
}

bool AuthManager::Check(const std::string& user, const std::string& passwd) const {
	if (!openssl::LibCryptoAvailable()) {
		return false;
	}

	const auto [it, end] = findToken(user);
	if (it == end) {
		return false;
	}

	unsigned char hash[SHA256_DIGEST_LENGTH];
	reindexer::shared_lock lk(mtx_);
	return it->second.check(Token::generate(passwd, hash), *this);
}

void AuthManager::Refresh(const std::string& user, const std::string& passwd) {
	if (!openssl::LibCryptoAvailable()) {
		return;
	}

	const auto [it, end] = findToken(user);
	if (it == end) {
		return;
	}

	unsigned char hash[SHA256_DIGEST_LENGTH];
	reindexer::unique_lock lk(mtx_);
	it->second.refresh(Token::generate(passwd, hash), *this);
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

bool AuthManager::Token::check(std::string_view token, const AuthManager&) const noexcept {
	return token == token_ && reindexer::system_clock_w::now() - lastUpd_ < std::chrono::hours(1);
}

void AuthManager::Token::refresh(std::string_view token, const AuthManager&) {
	if (token_.empty()) {
		token_ = token;
	}

	if (token != token_) {
		throw Error(errParams, "Incorrect auth-token for refresh");
	}

	lastUpd_ = reindexer::system_clock_w::now();
}

}  // namespace reindexer_server
