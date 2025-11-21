#include "dbmanager.h"
#include "estl/gift_str.h"
#include "estl/lock.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/catch_and_return.h"
#include "tools/crypt.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "vendor/yaml-cpp/yaml.h"

namespace reindexer_server {

using namespace std::string_view_literals;

static const std::string kUsersYAMLFilename = "users.yml";
static const std::string kUsersJSONFilename = "users.json";

DBManager::DBManager(const ServerConfig& config, IClientsStats* clientsStats)
	: config_(config), storageType_(datastorage::StorageType::LevelDB), clientsStats_(clientsStats) {}

Error DBManager::Init() RX_NO_THREAD_SAFETY_ANALYSIS {
	if (config_.EnableSecurity) {
		auto status = readUsers();
		if (!status.ok()) {
			return status;
		}

		authManager_.Init(*this);
	}

	std::vector<fs::DirEntry> foundDb;
	if (!config_.StoragePath.empty() && fs::ReadDir(config_.StoragePath, foundDb) < 0) {
		return Error(errParams, "Can't read reindexer dir {}", config_.StoragePath);
	}

	try {
		storageType_ = datastorage::StorageTypeFromString(config_.StorageEngine);
	} catch (const Error& err) {
		return err;
	}

	for (auto& de : foundDb) {
		if (de.isDir && validateObjectName(de.name, false)) {
			auto status = loadOrCreateDatabase(de.name, config_.StartWithErrors);
			if (!status.ok()) {
				logFmt(LogError, "Failed to open database '{}' - {}", de.name, status.what());
				if (status.code() == errNotValid) {
					logFmt(LogError, "Try to run:\t`reindexer_tool --dsn \"builtin://{}\" --repair`  to restore data", config_.StoragePath);
					return status;
				}
			}
		}
	}

	return {};
}

Error DBManager::OpenDatabase(const std::string& dbName, AuthContext& auth, bool canCreate) {
	RdxContext dummyCtx;
	auto status = Login(dbName, auth);
	if (!status.ok()) {
		return status;
	}
	auto dbConnect = [&auth](Reindexer* db) {
		if (auth.checkClusterID_) {
			return db->Connect(std::string(), ConnectOpts().WithExpectedClusterID(auth.expectedClusterID_));
		}
		return Error();
	};

	{
		smart_lock<Mutex> lck(mtx_, dummyCtx, NonUnique);
		auto it = dbs_.find(dbName);
		if (it != dbs_.end()) {
			status = dbConnect(it->second.get());
			if (!status.ok()) {
				return status;
			}
			auth.db_ = it->second.get();
			return Error();
		}
	}

	if (!canCreate) {
		return Error(errNotFound, "Database '{}' not found", dbName);
	}
	if (auth.role_ < kRoleOwner) {
		return Error(errForbidden, "Forbidden to create database '{}'", dbName);
	}
	if (!validateObjectName(dbName, false)) {
		return Error(errParams, "Database name '{}' contains invalid character. Only alphas, digits,'_','-', are allowed", dbName);
	}

	smart_lock<Mutex> lck(mtx_, dummyCtx, Unique);
	auto it = dbs_.find(dbName);
	if (it != dbs_.end()) {
		status = dbConnect(it->second.get());
		if (!status.ok()) {
			return status;
		}
		auth.db_ = it->second.get();
		return Error();
	}

	status = loadOrCreateDatabase(dbName, true, auth);
	if (!status.ok()) {
		return status;
	}

	it = dbs_.find(dbName);
	assertrx(it != dbs_.end());
	auth.db_ = it->second.get();
	return Error();
}

Error DBManager::loadOrCreateDatabase(const std::string& dbName, bool allowDBErrors, const AuthContext& auth) {
	if (clustersShutDown_) {
		return Error(errTerminated, "DBManager is already preparing for shutdown");
	}

	std::string storagePath = !config_.StoragePath.empty() ? fs::JoinPath(config_.StoragePath, dbName) : "";

	logFmt(LogInfo, "Loading database {}", dbName);
	auto db = std::make_unique<reindexer::Reindexer>(
		reindexer::ReindexerConfig().WithClientStats(clientsStats_).WithUpdatesSize(config_.MaxUpdatesSize).WithDBName(dbName));
	StorageTypeOpt storageType = kStorageTypeOptLevelDB;
	switch (storageType_) {
		case datastorage::StorageType::LevelDB:
			storageType = kStorageTypeOptLevelDB;
			break;
		case datastorage::StorageType::RocksDB:
			storageType = kStorageTypeOptRocksDB;
			break;
	}
	auto opts = ConnectOpts().AllowNamespaceErrors(allowDBErrors).WithStorageType(storageType);
	if (auth.checkClusterID_) {
		opts = opts.WithExpectedClusterID(auth.expectedClusterID_);
	}
	auto status = db->Connect(storagePath, opts);
	if (status.ok()) {
		dbs_[dbName] = std::move(db);
	}

	return status;
}

Error DBManager::DropDatabase(AuthContext& auth) {
	{
		Reindexer* db = nullptr;
		auto status = auth.GetDB<AuthContext::CalledFrom::Core>(kRoleOwner, &db);
		if (!status.ok()) {
			return status;
		}
	}
	const std::string& dbName = auth.dbName_;

	reindexer::unique_lock lck(mtx_);
	auto it = dbs_.find(dbName);
	if (it == dbs_.end()) {
		return Error(errParams, "Database {} not found", dbName);
	}
	dbs_.erase(it);
	std::ignore = fs::RmDirAll(fs::JoinPath(config_.StoragePath, dbName));
	auth.ResetDB();

	return {};
}

std::vector<std::string> DBManager::EnumDatabases() {
	std::vector<std::string> dbs;

	reindexer::shared_lock lck(mtx_);
	dbs.reserve(dbs_.size());
	for (const auto& it : dbs_) {
		dbs.emplace_back(it.first);
	}
	return dbs;
}

void DBManager::ShutdownClusters() {
	reindexer::unique_lock lck(mtx_);
	if (!clustersShutDown_) {
		for (auto& db : dbs_) {
			auto err = db.second->ShutdownCluster();
			if (!err.ok()) {
				logFmt(LogError, "Error on cluster shutdown: %s", err.whatStr());
			}
		}
		clustersShutDown_ = true;
	}
}

Error DBManager::Login(const std::string& dbName, AuthContext& auth) {
	if (kRoleSystem == auth.role_) {
		auth.dbName_ = dbName;
		return {};
	}

	if (IsNoSecurity()) {
		auth.role_ = kRoleOwner;
		auth.dbName_ = dbName;
		return {};
	}

	if (auth.role_ != kUnauthorized && dbName == auth.dbName_) {
		return {};
	}

	auto it = users_.find(auth.login_.Str());
	if (it == users_.end()) {
		return Error(errForbidden, "Unauthorized");
	}

	if (!authorized(auth)) {
		// TODO change to SCRAM-RSA
		if (!it->second.salt.empty()) {
			std::string hash;
			if (it->second.algorithm == reindexer::HashAlgorithm::MD5) {
				hash = reindexer::MD5crypt(auth.password_, it->second.salt);
			} else if (!auth.password_.empty()) {
				if (!openssl::LibCryptoAvailable()) {
					return Error(errSystem,
								 "It is not possible to use some necessary functions from the installed openssl library. Try updating the "
								 "library or installing last dev-package");
				}
				std::string dummySalt;
				HashAlgorithm dummyAlg;
				auto err =
					ParseCryptString(shacrypt(auth.password_.data(), it->second.algorithm == reindexer::HashAlgorithm::SHA256 ? "5" : "6",
											  it->second.salt.data()),
									 hash, dummySalt, dummyAlg);

				if (!err.ok()) {
					return Error(errForbidden, "Unauthorized: {}", err.what());
				}
			}

			if (it->second.hash != hash) {
				return Error(errForbidden, "Unauthorized");
			}

			authManager_.Refresh(auth.login_.Str(), auth.password_);
		} else if (it->second.hash != auth.password_) {
			return Error(errForbidden, "Unauthorized");
		}
	}

	auth.role_ = kRoleNone;

	if (!dbName.empty()) {
		const UserRecord& urec = it->second;

		auto dbIt = urec.roles.find("*");
		if (dbIt != urec.roles.end()) {
			auth.role_ = dbIt->second;
		}

		dbIt = urec.roles.find(dbName);
		if (dbIt != urec.roles.end() && dbIt->second > auth.role_) {
			auth.role_ = dbIt->second;
		}
	}
	auth.dbName_ = dbName;

	return {};
}

bool DBManager::authorized(const AuthContext& auth) { return authManager_.Check(auth.login_.Str(), auth.password_); }

Error DBManager::readUsers() noexcept {
	users_.clear();
	Error jResult;
	auto yResult = readUsersYAML();
	if (!yResult.ok()) {
		jResult = readUsersJSON();
		if (yResult.code() == errNotFound && jResult.code() == errNotFound) {
			return createDefaultUsersYAML();
		}
	}
	return (yResult.code() == errNotFound) ? jResult : yResult;
}

Error DBManager::readUsersYAML() noexcept {
	try {
		std::string content;
		int res = fs::ReadFile(fs::JoinPath(config_.StoragePath, kUsersYAMLFilename), content);
		if (res < 0) {
			return Error(errNotFound, "Can't read '{}' file", kUsersYAMLFilename);
		}
		YAML::ScannerOpts opts;
		opts.disableAnchors = true;
		YAML::Node root = YAML::Load(content, opts);
		for (const auto& user : root) {
			UserRecord urec;
			urec.login = user.first.as<std::string>();
			auto userNode = user.second;
			auto err = ParseCryptString(userNode["hash"].as<std::string>(), urec.hash, urec.salt, urec.algorithm);
			if (!err.ok()) {
				logFmt(LogWarning, "Hash parsing error for user '{}': {}", urec.login, err.what());
				continue;
			}
			auto userRoles = userNode["roles"];
			if (userRoles.IsMap()) {
				for (const auto& role : userRoles) {
					std::string db(role.first.as<std::string>());
					try {
						urec.roles.emplace(db, userRoleFromString(role.second.as<std::string>()));
					} catch (const Error& err) {
						logFmt(LogWarning, "Skipping user '{}' for db '{}': ", urec.login, db, err.what());
					}
				}
				if (urec.roles.empty()) {
					logFmt(LogWarning, "User '{}' doesn't have valid roles", urec.login);
				} else {
					users_.emplace(urec.login, urec);
				}
			} else {
				logFmt(LogWarning, "Skipping user '{}': no 'roles' node found", urec.login);
			}
		}
	} catch (const std::exception& ex) {
		return Error(errParseYAML, "Users: {}", ex.what());
	}
	return Error();
}

Error DBManager::readUsersJSON() noexcept {
	std::string content;
	int res = fs::ReadFile(fs::JoinPath(config_.StoragePath, kUsersJSONFilename), content);
	if (res < 0) {
		return Error(errNotFound, "Can't read '{}' file", kUsersJSONFilename);
	}

	try {
		gason::JsonParser parser;
		auto root = parser.Parse(giftStr(content));
		for (auto& userNode : root) {
			UserRecord urec;
			urec.login = std::string(userNode.key);
			auto err = ParseCryptString(userNode["hash"].As<std::string>(), urec.hash, urec.salt, urec.algorithm);
			if (!err.ok()) {
				logFmt(LogWarning, "Hash parsing error for user '{}': {}", urec.login, err.what());
				continue;
			}
			for (auto& roleNode : userNode["roles"]) {
				std::string db(roleNode.key);
				try {
					UserRole role = userRoleFromString(roleNode.As<std::string_view>());
					urec.roles.emplace(db, role);
				} catch (const Error& err) {
					logFmt(LogWarning, "Skipping user '{}' for db '{}': ", urec.login, db, err.what());
				}
			}
			if (urec.roles.empty()) {
				logFmt(LogWarning, "User '{}' doesn't have valid roles", urec.login);
			} else {
				users_.emplace(urec.login, urec);
			}
		}
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "Users: {}", ex.what());
	}
	return errOK;
}

Error DBManager::createDefaultUsersYAML() noexcept {
	try {
		logFmt(LogInfo, "Creating default {} file", kUsersYAMLFilename);
		int64_t res = fs::WriteFile(
			fs::JoinPath(config_.StoragePath, kUsersYAMLFilename),
			"# List of db's users, their's roles and privileges\n\n"
			"# Anchors and aliases do not work here due to compatibility reasons\n\n"
			"# Username\n"
			"reindexer:\n"
			"  # Hash may be generated via openssl tool - `openssl passwd -<type> -salt MySalt MyPassword`\n"
			"  # <type> can take one of the following values:\n"
			"  #    1 - MD5-based password algorithm\n"
			"  #    5 - SHA256-based password algorithm\n"
			"  #    6 - SHA512-based password algorithm\n"
			"  #    For values 5 and 6 of <type> to work correctly, you should have the openssl library dev-package installed in the "
			"system\n"
			"  # If hash doesn't start with '$' sign it will be used as raw password itself\n"
			"  hash: $1$rdxsalt$VIR.dzIB8pasIdmyVGV0E/\n"
			"  # User's roles for specific databases, * in place of db name means any database\n"
			"  # Allowed roles:\n"
			"  # 1) data_read - user can read data from database\n"
			"  # 2) data_write - user can write data to database\n"
			"  # 3) db_admin - user can manage database: kRoleDataWrite + create & delete namespaces, modify indexes\n"
			"  # 4) replication - same as db_admin but with restrictions on use in various protocols and "
			"additional context checks for replication. This role should be used for the asynchronous and synchronous (RAFT-cluster) "
			"replication instead of db_admin/owner\n"
			"  # 5) sharding - same as db_admin but with restrictions on use in various protocols and additional "
			"context checks for sharding. This role should be used for the sharding interconnections instead of db_admin/owner\n"
			"  # 6) owner - user has all privileges on database: kRoleDBAdmin + create & drop database\n"
			"  roles:\n"
			"    *: owner\n");
		if (res < 0) {
			return Error(errParams, "Unable to write default config file: {}", strerror(errno));
		}
		users_.emplace("reindexer", UserRecord{"reindexer", "VIR.dzIB8pasIdmyVGV0E/", "rdxsalt", {{"*", kRoleOwner}}, HashAlgorithm::MD5});
	}
	CATCH_AND_RETURN;
	return Error();
}

UserRole DBManager::userRoleFromString(std::string_view strRole) {
	if (strRole == "data_read"sv) {
		return kRoleDataRead;
	} else if (strRole == "data_write"sv) {
		return kRoleDataWrite;
	} else if (strRole == "db_admin"sv) {
		return kRoleDBAdmin;
	} else if (strRole == "replication"sv) {
		return kRoleReplication;
	} else if (strRole == "sharding"sv) {
		return kRoleSharding;
	} else if (strRole == "owner"sv) {
		return kRoleOwner;
	}
	throw Error(errParams, "Role \'{}\' is invalid", strRole);
}

std::string_view UserRoleName(UserRole role) noexcept {
	switch (role) {
		case kUnauthorized:
			return "unauthorized"sv;
		case kRoleNone:
			return "none"sv;
		case kRoleDataRead:
			return "data_read"sv;
		case kRoleDataWrite:
			return "data_write"sv;
		case kRoleDBAdmin:
			return "db_admin"sv;
		case kRoleReplication:
			return "replication"sv;
		case kRoleSharding:
			return "sharding"sv;
		case kRoleOwner:
			return "owner"sv;
		case kRoleSystem:
			return "system"sv;
	}
	return ""sv;
}

}  // namespace reindexer_server
