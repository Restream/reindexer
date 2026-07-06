#pragma once

#include <string>
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "tools/clock.h"

namespace reindexer_server {

class DBManager;

class [[nodiscard]] AuthManager {
public:
	AuthManager() = default;
	void Init(const DBManager& dbmanager);

	bool Check(const std::string& user, const std::string& passwd) const RX_REQUIRES(!mtx_);
	void Refresh(const std::string& user, const std::string& passwd) RX_REQUIRES(!mtx_);

private:
	class [[nodiscard]] Token {
		friend class AuthManager;

		static std::string_view generate(const std::string& passwd, unsigned char* hash);
		bool check(std::string_view token, const AuthManager& host) const noexcept RX_REQUIRES_SHARED(host.mtx_);
		void refresh(std::string_view token, const AuthManager& host) RX_REQUIRES(host.mtx_);

		std::string token_;
		reindexer::system_clock_w::time_point lastUpd_ = reindexer::system_clock_w::now();
	};
	auto findToken(const std::string& user) const&;
	auto findToken(const std::string& user) &;
	auto findToken(const std::string& user) const&& = delete;

	mutable reindexer::shared_mutex mtx_;
	reindexer::fast_hash_map<std::string, Token> userAuthData_ RX_GUARDED_BY(mtx_);
};
}  // namespace reindexer_server
