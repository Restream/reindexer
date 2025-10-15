#include "auth_tools.h"

bool WithSecurity() noexcept { return std::getenv("RX_TEST_SECURITY_REQUIRED"); }

std::string TLSPath() noexcept {
	auto path = std::getenv("RX_TEST_TLS_PATH");
	assertrx(path);
	return path;
}

reindexer::fast_hash_map<reindexer_server::UserRole, TestUserDataFactory::User>& TestUserDataFactory::Get(int serverId) {
	using namespace reindexer_server;
	if (auto it = users_.find(serverId); it != users_.end()) {
		return it->second;
	}

	auto pair = [serverId](UserRole role) { return std::make_pair(role, User{user(role, serverId), passwd(role, serverId)}); };

	auto [it, _] = users_.insert({serverId,
								  {
									  pair(UserRole::kRoleReplication),
									  pair(UserRole::kRoleSharding),
									  pair(UserRole::kRoleDBAdmin),
								  }});

	return it->second;
}

std::string TestUserDataFactory::user(reindexer_server::UserRole role, int serverId) {
	return fmt::format(loginTmplt, reindexer_server::UserRoleName(role), serverId);
}

std::string TestUserDataFactory::passwd(reindexer_server::UserRole role, int serverId) {
	return fmt::format(passwdTmplt, int(role), serverId);
}

std::string TestUserDataFactory::dump(reindexer_server::UserRole role, int serverId) {
	auto& user = Get(serverId)[role];
	return fmt::format("{}:{}@", user.login, user.password);
}

reindexer::DSN MakeDsn(reindexer_server::UserRole role, int serverId, int port, const std::string& db) {
	return reindexer::DSN(fmt::format("cproto{}://{}127.0.0.1:{}/{}", WithSecurity() ? "s" : "",
									  WithSecurity() ? TestUserDataFactory::dump(role, serverId) : "", port, db));
}

reindexer::fast_hash_map<int, reindexer::fast_hash_map<reindexer_server::UserRole, TestUserDataFactory::User>> TestUserDataFactory::users_;
