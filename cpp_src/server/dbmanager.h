#pragma once

#include <string>
#include <thread>
#include <unordered_map>
#include "core/reindexer.h"
#include "core/storage/storagetype.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "server/config.h"
#include "tools/stringstools.h"

namespace reindexer {
class IClientsStats;
}

/// @namespace reindexer_server
/// The namespace for reindexer server implementation
namespace reindexer_server {

using namespace reindexer;

/// Possible user roles
enum UserRole {
	kUnauthorized,	 /// User is not authorized
	kRoleNone,		 /// User is authenticaTed, but has no any righs
	kRoleDataRead,	 /// User can read data from database
	kRoleDataWrite,	 /// User can write data to database
	kRoleDBAdmin,	 /// User can manage database: kRoleDataWrite + create & delete namespaces, modify indexes
	kRoleOwner,		 /// User has all privilegies on database: kRoleDBAdmin + create & drop database
	kRoleSystem,	 /// Special role for internal usage
};

std::string_view UserRoleName(UserRole role) noexcept;

/// Record about user credentials
struct UserRecord {
	std::string login;								  /// User's login
	std::string hash;								  /// User's password or hash
	std::string salt;								  /// Password salt
	std::unordered_map<std::string, UserRole> roles;  /// map of user's roles on databases
};

class DBManager;

class AuthContext;
static AuthContext MakeSystemAuthContext();

/// Context of user authentification
class AuthContext {
	friend DBManager;
	friend AuthContext MakeSystemAuthContext();

public:
	/// Constuct empty context
	AuthContext() = default;
	/// Construct context with user credentials
	/// @param login - User's login
	/// @param password - User's password
	AuthContext(const std::string &login, const std::string &password) : login_(login), password_(password) {}
	/// Set expected leader's replication clusted ID
	/// @param clusterID - Expected cluster ID value
	void SetExpectedClusterID(int clusterID) {
		checkClusterID_ = true;
		expectedClusterID_ = clusterID;
	}
	/// Check if reqired role meets role from context, and get pointer to Reindexer DB object
	/// @param role - Requested role one of UserRole enum
	/// @param ret - Pointer to returned database pointer
	/// @return Error - error object
	Error GetDB(UserRole role, Reindexer **ret) noexcept {
		if (role > role_)
			return Error(errForbidden, "Forbidden: need role %s of db '%s' user '%s' have role=%s", UserRoleName(role), dbName_, login_,
						 UserRoleName(role_));
		*ret = db_;
		return errOK;
	}
	/// Reset Reindexer DB object pointer in context
	void ResetDB() {
		db_ = nullptr;
		dbName_.clear();
	}
	/// Check does context have an valid Reindexer DB object pointer
	/// @return true if db is ok
	bool HaveDB() { return db_ != nullptr; }
	/// Get user login
	/// @return user login
	const std::string &Login() { return login_; }
	/// Get database name
	/// @return db name
	const std::string &DBName() { return dbName_; }
	/// Get user rights
	/// @return user rights
	UserRole UserRights() const { return role_; }

protected:
	AuthContext(UserRole role) : role_(role) {}

	std::string login_;
	std::string password_;
	UserRole role_ = kUnauthorized;
	std::string dbName_;
	int expectedClusterID_ = -1;
	bool checkClusterID_ = false;
	Reindexer *db_ = nullptr;
};

static inline AuthContext MakeSystemAuthContext() { return AuthContext(kRoleSystem); }

/// Database manager. Control's available databases, users and their roles
class DBManager {
public:
	/// Construct DBManager
	/// @param config - server config reference
	/// @param clientsStats - object for receiving clients statistics
	DBManager(const ServerConfig &config, IClientsStats *clientsStats = nullptr);
	/// Initialize database:
	/// Read all found databases to RAM
	/// Read user's database
	/// @param storageEngine - underlying storage engine ("leveldb"/"rocksdb")
	/// @param allowDBErrors - true: Ignore errors during existing DBs load; false: Return error if error occures during DBs load
	/// @param withAutorepair - true: Enable storage autorepair feature for this DB; false: Disable storage autorepair feature for this DB
	/// @return Error - error object
	Error Init();
	/// Authenticate user, and grant roles to database with specified dbName
	/// @param dbName - database name. Can be empty.
	/// @param auth - AuthContext with user credentials
	/// @return Error - error object
	Error Login(const std::string &dbName, AuthContext &auth);
	/// Open database and authentificate user
	/// @param dbName - database name, Can't be empty
	/// @param auth - AuthContext filled with user credentials or already authorized AuthContext
	/// @param canCreate - true: Create database, if not exists; false: return error, if database not exists
	/// @return Error - error object
	Error OpenDatabase(const std::string &dbName, AuthContext &auth, bool canCreate);
	/// Drop database from disk storage and memory. Reindexer DB object will be destroyed
	/// @param auth - Authorized AuthContext, with valid Reindexer DB object and reasonale role
	/// @return Error - error object
	Error DropDatabase(AuthContext &auth);
	/// Check if security disabled
	/// @return bool - true: security checks are disabled; false: security checks are enabled
	bool IsNoSecurity() { return !config_.EnableSecurity; }
	/// Enum list of available databases
	/// @return names of available databases
	std::vector<std::string> EnumDatabases();

	void ShutdownClusters();

private:
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::DbManager>;
	Error readUsers() noexcept;
	Error readUsersYAML() noexcept;
	Error readUsersJSON() noexcept;
	Error createDefaultUsersYAML() noexcept;
	static UserRole userRoleFromString(std::string_view strRole);
	Error loadOrCreateDatabase(const std::string &name, bool allowDBErrors, bool withAutorepair, const AuthContext &auth = AuthContext());

	std::unordered_map<std::string, std::unique_ptr<Reindexer>, nocase_hash_str, nocase_equal_str> dbs_;
	std::unordered_map<std::string, UserRecord> users_;
	const ServerConfig &config_;
	Mutex mtx_;
	datastorage::StorageType storageType_;

	IClientsStats *clientsStats_ = nullptr;
	bool clustersShutDown_ = false;
};

}  // namespace reindexer_server
