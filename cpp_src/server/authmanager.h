#pragma once

#include <shared_mutex>
#include <string>
#include "estl/fast_hash_map.h"

namespace reindexer_server {

class DBManager;

class AuthManager {
public:
	AuthManager() = default;
	void Init(const DBManager &dbmanager);

	bool Check(const std::string &user, const std::string &passwd) const;
	void Refresh(const std::string &user, const std::string &passwd);

private:
	class Token {
		friend class AuthManager;

		static std::string_view generate(const std::string &passwd, unsigned char *hash);
		bool check(std::string_view token) const noexcept;
		void refresh(std::string_view token);

		std::string token_;
		std::chrono::time_point<std::chrono::system_clock> lastUpd_ = std::chrono::system_clock::now();
	};

	mutable std::shared_mutex mtx_;
	reindexer::fast_hash_map<std::string, Token> userAuthData_;
};
}  // namespace reindexer_server