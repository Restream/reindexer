#include "auth_tools.h"

reindexer::fast_hash_map<reindexer_server::UserRole, TestUserDataFactory::User>& TestUserDataFactory::Get(int serverId) noexcept {
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

reindexer::DSN MakeDsn(reindexer_server::UserRole role, int serverId, int port, const std::string& db) {
	return reindexer::DSN(fmt::format("cproto{}://{}127.0.0.1:{}/{}", WithSecurity() ? "s" : "",
									  WithSecurity() ? TestUserDataFactory::dump(role, serverId) : "", port, db));
}

reindexer::fast_hash_map<int, reindexer::fast_hash_map<reindexer_server::UserRole, TestUserDataFactory::User>> TestUserDataFactory::users_;