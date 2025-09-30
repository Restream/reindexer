#pragma once
#include "server/dbmanager.h"
#include "tools/dsn.h"

bool WithSecurity() noexcept;
std::string TLSPath() noexcept;

struct [[nodiscard]] TestUserDataFactory {
	struct [[nodiscard]] User {
		std::string login;
		std::string password;
	};

	static reindexer::fast_hash_map<reindexer_server::UserRole, User>& Get(int serverId);

private:
	friend reindexer::DSN MakeDsn(reindexer_server::UserRole role, int serverId, int port, const std::string& db);

	static std::string user(reindexer_server::UserRole role, int serverId);
	static std::string passwd(reindexer_server::UserRole role, int serverId);
	static std::string dump(reindexer_server::UserRole role, int serverId);

	static constexpr auto loginTmplt = "Test_{}_user{}";
	static constexpr auto passwdTmplt = "TestMaskingPassword{}{}";
	static reindexer::fast_hash_map<int, reindexer::fast_hash_map<reindexer_server::UserRole, User>> users_;
};

reindexer::DSN MakeDsn(reindexer_server::UserRole role, int serverId, int port, const std::string& db);

template <typename ServerControlInterfacePtr>
reindexer::DSN MakeDsn(reindexer_server::UserRole role, const ServerControlInterfacePtr& sc) {
	return MakeDsn(role, sc->Id(), sc->RpcPort(), sc->DbName());
}

namespace reindexer {
inline bool operator==(const reindexer::DSN& lhs, const reindexer::DSN& rhs) noexcept {
	return (&reindexer::operator== <reindexer::DSN>)(lhs, rhs) || fmt::format("{}", lhs) == fmt::format("{}", rhs);
}

inline bool operator==(const std::string& lhs, const reindexer::DSN& rhs) noexcept {
	const auto& DsnFromStr = reindexer::DSN(lhs);
	return (&reindexer::operator== <reindexer::DSN>)(DsnFromStr, rhs) || fmt::format("{}", DsnFromStr) == fmt::format("{}", rhs);
}

inline bool operator!=(const reindexer::DSN& lhs, const reindexer::DSN& rhs) noexcept { return !(lhs == rhs); }
}  // namespace reindexer
