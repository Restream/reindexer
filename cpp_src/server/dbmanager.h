#pragma once

#include <unordered_map>
#include "authmanager.h"
#include "core/reindexer.h"
#include "core/storage/storagetype.h"
#include "estl/marked_mutex.h"
#include "estl/shared_mutex.h"
#include "server/config.h"
#include "tools/crypt.h"
#include "tools/masking.h"
#include "tools/stringstools.h"

namespace reindexer {
class IClientsStats;
}

/// @namespace reindexer_server
/// The namespace for reindexer server implementation
namespace reindexer_server {

using namespace reindexer;

/// Possible user roles
enum [[nodiscard]] UserRole {
	kUnauthorized,	   /// User is not authorized
	kRoleNone,		   /// User is authenticated, but has no any rights
	kRoleDataRead,	   /// User can read data from database
	kRoleDataWrite,	   /// User can write data to database
	kRoleDBAdmin,	   /// User can manage database: kRoleDataWrite + create & delete namespaces, modify indexes
	kRoleReplication,  /// Same as kRoleDBAdmin but with restrictions on use in various protocols and additional context checks for
					   /// replication
	kRoleSharding,	   /// Same as kRoleDBAdmin but with restrictions on use in various protocols and additional context checks for sharding
	kRoleOwner,		   /// User has all privileges on database: kRoleDBAdmin + create & drop database
	kRoleSystem,	   /// Special role for internal usage
};

std::string_view UserRoleName(UserRole role) noexcept;

/// Record about user credentials
struct [[nodiscard]] UserRecord {
	std::string login;								  /// User's login
	std::string hash;								  /// User's password or hash
	std::string salt;								  /// Password salt
	std::unordered_map<std::string, UserRole> roles;  /// map of user's roles on databases
	HashAlgorithm algorithm;						  /// Hash calculation algorithm
};

class DBManager;

class AuthContext;
static AuthContext MakeSystemAuthContext();

/// Context of user authentication
class [[nodiscard]] AuthContext {
	friend DBManager;
	friend AuthContext MakeSystemAuthContext();

public:
	class [[nodiscard]] UserLogin {
	public:
		UserLogin() noexcept = default;

		UserLogin(std::string login) noexcept : login_(std::move(login)) {}
		UserLogin(const char* login) noexcept : login_(login) {}

		const std::string& Str() const& noexcept { return login_; }
		const std::string& Str() const&& = delete;

	private:
		std::string login_;
	};

	/// Construct empty context
	AuthContext() = default;
	/// Construct context with user credentials
	/// @param login - User's login
	/// @param password - User's password
	AuthContext(const std::string& login, const std::string& password) : login_(login), password_(password) {}
	/// Set expected leader's replication cluster ID
	/// @param clusterID - Expected cluster ID value
	void SetExpectedClusterID(int clusterID) noexcept {
		checkClusterID_ = true;
		expectedClusterID_ = clusterID;
	}

	enum class [[nodiscard]] CalledFrom { Core, RPCServer, HTTPServer, GRPC };

	/// Check if required role meets role from context, and get pointer to Reindexer DB object
	/// @param role - Requested role one of UserRole enum
	/// @param ret - Pointer to returned database pointer
	/// @return Error - error object
	template <CalledFrom caller, typename... Args>
	Error GetDB(UserRole role, Reindexer** ret, Args&&... args) const noexcept;
	/// Reset Reindexer DB object pointer in context
	void ResetDB() noexcept {
		db_ = nullptr;
		dbName_.clear();
	}
	/// Check does context have an valid Reindexer DB object pointer
	/// @return true if db is ok
	bool HaveDB() const noexcept { return db_ != nullptr; }
	/// Get user login
	/// @return user login
	const UserLogin& Login() const& noexcept { return login_; }
	const UserLogin& Login() const&& = delete;
	/// Get database name
	/// @return db name
	const std::string& DBName() const& noexcept { return dbName_; }
	const std::string& DBName() const&& = delete;
	/// Get user rights
	/// @return user rights
	UserRole UserRights() const noexcept { return role_; }

protected:
	AuthContext(UserRole role) noexcept : role_(role) {}

	UserLogin login_;
	std::string password_;
	UserRole role_ = kUnauthorized;
	std::string dbName_;
	int expectedClusterID_ = -1;
	bool checkClusterID_ = false;
	Reindexer* db_ = nullptr;
};

static inline AuthContext MakeSystemAuthContext() { return AuthContext(kRoleSystem); }

/// Database manager. Control's available databases, users and their roles
class [[nodiscard]] DBManager {
public:
	/// Construct DBManager
	/// @param config - server config reference
	/// @param clientsStats - object for receiving clients statistics
	DBManager(const ServerConfig& config, IClientsStats* clientsStats = nullptr);
	/// Initialize database:
	/// Read all found databases to RAM
	/// Read user's database
	/// @param storageEngine - underlying storage engine ("leveldb"/"rocksdb")
	/// @param allowDBErrors - true: Ignore errors during existing DBs load; false: Return error if error occurs during DBs load
	/// @return Error - error object
	Error Init();
	/// Authenticate user, and grant roles to database with specified dbName
	/// @param dbName - database name. Can be empty.
	/// @param auth - AuthContext with user credentials
	/// @return Error - error object
	Error Login(const std::string& dbName, AuthContext& auth);
	/// Open database and authenticate user
	/// @param dbName - database name, Can't be empty
	/// @param auth - AuthContext filled with user credentials or already authorized AuthContext
	/// @param canCreate - true: Create database, if not exists; false: return error, if database not exists
	/// @return Error - error object
	Error OpenDatabase(const std::string& dbName, AuthContext& auth, bool canCreate) RX_REQUIRES(!mtx_);
	/// Drop database from disk storage and memory. Reindexer DB object will be destroyed
	/// @param auth - Authorized AuthContext, with valid Reindexer DB object and reasonable role
	/// @return Error - error object
	Error DropDatabase(AuthContext& auth) RX_REQUIRES(!mtx_);
	/// Check if security disabled
	/// @return bool - true: security checks are disabled; false: security checks are enabled
	bool IsNoSecurity() { return !config_.EnableSecurity; }
	/// Enum list of available databases
	/// @return names of available databases
	std::vector<std::string> EnumDatabases() RX_REQUIRES(!mtx_);

	void ShutdownClusters() RX_REQUIRES(!mtx_);

private:
	friend class AuthManager;
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::DbManager>;
	Error readUsers() noexcept;
	Error readUsersYAML() noexcept;
	Error readUsersJSON() noexcept;
	Error createDefaultUsersYAML() noexcept;
	static UserRole userRoleFromString(std::string_view strRole);
	Error loadOrCreateDatabase(const std::string& name, bool allowDBErrors, const AuthContext& auth = AuthContext()) RX_REQUIRES(mtx_);

	std::unordered_map<std::string, std::unique_ptr<Reindexer>, nocase_hash_str, nocase_equal_str> dbs_ RX_GUARDED_BY(mtx_);
	std::unordered_map<std::string, UserRecord> users_;
	const ServerConfig& config_;
	Mutex mtx_;
	datastorage::StorageType storageType_;

	IClientsStats* clientsStats_ = nullptr;
	bool clustersShutDown_ = false;

	bool authorized(const AuthContext& auth);

	AuthManager authManager_;
};

template <typename Cout, typename UserLoginT>
std::enable_if_t<std::is_same_v<UserLoginT, AuthContext::UserLogin>, Cout>& operator<<(Cout& cout, const UserLoginT& login) {
	cout << maskLogin(login.Str());
	return cout;
}

}  // namespace reindexer_server

template <>
struct [[nodiscard]] fmt::formatter<reindexer_server::AuthContext::UserLogin> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer_server::AuthContext::UserLogin& login, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "{}", reindexer::maskLogin(login.Str()));
	}
};

namespace reindexer_server {

template <AuthContext::CalledFrom caller, typename... Args>
Error AuthContext::GetDB(UserRole role, Reindexer** ret, Args&&... args) const noexcept {
	if (role > role_) {
		return Error(errForbidden, "Forbidden: need role {} of db '{}' user '{}' have role={}", UserRoleName(role), dbName_, login_,
					 UserRoleName(role_));
	}

	const bool shardingRole = role_ == UserRole::kRoleSharding;
	const bool replicationRole = role_ == UserRole::kRoleReplication;

	if constexpr (caller == CalledFrom::RPCServer) {
		if (role > kRoleDataRead) {
			return [&](lsn_t lsn, int emitterServerId, int shardId) -> Error {
				if ((replicationRole && lsn.isEmpty() && emitterServerId < 0) || (shardingRole && shardId < 0)) [[unlikely]] {
					return Error(errForbidden, "Forbidden: {} is required to perform modify operation with the role '{}'",
								 replicationRole ? "a non-empty lsn or emitter server id" : "a non-negative shardId", UserRoleName(role_));
				}
				*ret = db_;
				return {};
			}(std::forward<Args>(args)...);
		}
	} else {
		if (shardingRole || replicationRole) [[unlikely]] {
			return Error(errForbidden, "Forbidden: incorrect role{}: '{}'", caller == CalledFrom::HTTPServer ? " in the HTTP protocol" : "",
						 UserRoleName(role_));
		}
	}
	*ret = db_;
	return {};
}

}  // namespace reindexer_server
