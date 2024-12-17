#pragma once
#include "server/dbmanager.h"
#include "tools/dsn.h"

static inline bool WithSecurity() noexcept { return std::getenv("RX_TEST_SECURITY_REQUIRED"); }
static inline std::string TLSPath() noexcept {
	auto path = std::getenv("RX_TEST_TLS_PATH");
	assertrx(path);
	return path;
}

struct TestUserDataFactory {
	struct User {
		std::string login;
		std::string password;
	};

	static reindexer::fast_hash_map<reindexer_server::UserRole, User>& Get(int serverId) noexcept;

private:
	friend reindexer::DSN MakeDsn(reindexer_server::UserRole role, int serverId, int port, const std::string& db);

	static std::string user(reindexer_server::UserRole role, int serverId) noexcept {
		return fmt::sprintf(loginTmplt, reindexer_server::UserRoleName(role), serverId);
	}
	static std::string passwd(reindexer_server::UserRole role, int serverId) noexcept {
		return fmt::sprintf(passwdTmplt, int(role), serverId);
	}

	static std::string dump(reindexer_server::UserRole role, int serverId) noexcept {
		auto& user = Get(serverId)[role];
		return fmt::sprintf("%s:%s@", user.login, user.password);
	}

	static constexpr auto loginTmplt = "Test_%s_user%i";
	static constexpr auto passwdTmplt = "TestMaskingPassword%i%i";
	static reindexer::fast_hash_map<int, reindexer::fast_hash_map<reindexer_server::UserRole, User>> users_;
};

reindexer::DSN MakeDsn(reindexer_server::UserRole role, int serverId, int port, const std::string& db);

template <typename ServerControlInterfacePtr>
reindexer::DSN MakeDsn(reindexer_server::UserRole role, const ServerControlInterfacePtr& sc) {
	return MakeDsn(role, sc->Id(), sc->RpcPort(), sc->DbName());
}

namespace reindexer {
inline bool operator==(const reindexer::DSN& lhs, const reindexer::DSN& rhs) noexcept {
	return (&reindexer::operator== <reindexer::DSN>)(lhs, rhs) || fmt::sprintf("%s", lhs) == fmt::sprintf("%s", rhs);
}

inline bool operator==(const std::string& lhs, const reindexer::DSN& rhs) noexcept {
	const auto& DsnFromStr = reindexer::DSN(lhs);
	return (&reindexer::operator== <reindexer::DSN>)(DsnFromStr, rhs) || fmt::sprintf("%s", DsnFromStr) == fmt::sprintf("%s", rhs);
}

inline bool operator!=(const reindexer::DSN& lhs, const reindexer::DSN& rhs) noexcept { return !(lhs == rhs); }
}  // namespace reindexer

// }
