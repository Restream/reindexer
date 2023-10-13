#include <fstream>
#include <mutex>

#include <thread>
#include "client/reindexerconfig.h"
#include "dbmanager.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/logger.h"
#include "tools/md5crypt.h"
#include "tools/stringstools.h"
#include "vendor/hash/md5.h"
#include "vendor/yaml-cpp/yaml.h"

namespace reindexer_server {

using namespace std::string_view_literals;

const std::string kUsersYAMLFilename = "users.yml";
const std::string kUsersJSONFilename = "users.json";

DBManager::DBManager(const ServerConfig &config, IClientsStats *clientsStats)
	: config_(config), storageType_(datastorage::StorageType::LevelDB), clientsStats_(clientsStats) {}

Error DBManager::Init() {
	if (config_.EnableSecurity) {
		auto status = readUsers();
		if (!status.ok()) {
			return status;
		}
	}

	std::vector<fs::DirEntry> foundDb;
	if (!config_.StoragePath.empty() && fs::ReadDir(config_.StoragePath, foundDb) < 0) {
		return Error(errParams, "Can't read reindexer dir %s", config_.StoragePath);
	}

	try {
		storageType_ = datastorage::StorageTypeFromString(config_.StorageEngine);
	} catch (const Error &err) {
		return err;
	}

	for (auto &de : foundDb) {
		if (de.isDir && validateObjectName(de.name, false)) {
			auto status = loadOrCreateDatabase(de.name, config_.StartWithErrors, config_.Autorepair);
			if (!status.ok()) {
				logPrintf(LogError, "Failed to open database '%s' - %s", de.name, status.what());
				if (status.code() == errNotValid) {
					logPrintf(LogError, "Try to run:\t`reindexer_tool --dsn \"builtin://%s\" --repair`  to restore data",
							  config_.StoragePath);
					return status;
				}
			}
		}
	}

	return {};
}

Error DBManager::OpenDatabase(const std::string &dbName, AuthContext &auth, bool canCreate) {
	RdxContext dummyCtx;
	auto status = Login(dbName, auth);
	if (!status.ok()) {
		return status;
	}
	auto dbConnect = [&auth](Reindexer *db) {
		if (auth.checkClusterID_) {
			return db->Connect(std::string(), ConnectOpts().WithExpectedClusterID(auth.expectedClusterID_));
		}
		return Error();
	};

	smart_lock<Mutex> lck(mtx_, dummyCtx);
	auto it = dbs_.find(dbName);
	if (it != dbs_.end()) {
		status = dbConnect(it->second.get());
		if (!status.ok()) return status;
		auth.db_ = it->second.get();
		return errOK;
	}
	lck.unlock();

	if (!canCreate) {
		return Error(errNotFound, "Database '%s' not found", dbName);
	}
	if (auth.role_ < kRoleOwner) {
		return Error(errForbidden, "Forbidden to create database '%s'", dbName);
	}
	if (!validateObjectName(dbName, false)) {
		return Error(errParams, "Database name '%s' contains invalid character. Only alphas, digits,'_','-', are allowed", dbName);
	}

	lck = smart_lock<Mutex>(mtx_, dummyCtx, true);
	it = dbs_.find(dbName);
	if (it != dbs_.end()) {
		status = dbConnect(it->second.get());
		if (!status.ok()) return status;
		auth.db_ = it->second.get();
		return errOK;
	}

	status = loadOrCreateDatabase(dbName, true, true, auth);
	if (!status.ok()) {
		return status;
	}

	it = dbs_.find(dbName);
	assertrx(it != dbs_.end());
	auth.db_ = it->second.get();
	return errOK;
}

Error DBManager::loadOrCreateDatabase(const std::string &dbName, bool allowDBErrors, bool withAutorepair, const AuthContext &auth) {
	if (clustersShutDown_) {
		return Error(errTerminated, "DBManager is already preparing for shutdown");
	}

	std::string storagePath = !config_.StoragePath.empty() ? fs::JoinPath(config_.StoragePath, dbName) : "";

	logPrintf(LogInfo, "Loading database %s", dbName);
	auto db = std::make_unique<reindexer::Reindexer>(reindexer::ReindexerConfig().WithClientStats(clientsStats_).WithUpdatesSize(config_.MaxUpdatesSize));
	StorageTypeOpt storageType = kStorageTypeOptLevelDB;
	switch (storageType_) {
		case datastorage::StorageType::LevelDB:
			storageType = kStorageTypeOptLevelDB;
			break;
		case datastorage::StorageType::RocksDB:
			storageType = kStorageTypeOptRocksDB;
			break;
	}
	auto opts = ConnectOpts().AllowNamespaceErrors(allowDBErrors).WithStorageType(storageType).Autorepair(withAutorepair);
	if (auth.checkClusterID_) {
		opts = opts.WithExpectedClusterID(auth.expectedClusterID_);
	}
	auto status = db->Connect(storagePath, opts);
	if (status.ok()) {
		dbs_[dbName] = std::move(db);
	}

	return status;
}

Error DBManager::DropDatabase(AuthContext &auth) {
	{
		Reindexer *db = nullptr;
		auto status = auth.GetDB(kRoleOwner, &db);
		if (!status.ok()) {
			return status;
		}
	}
	const std::string &dbName = auth.dbName_;

	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = dbs_.find(dbName);
	if (it == dbs_.end()) {
		return Error(errParams, "Database %s not found", dbName);
	}
	dbs_.erase(it);
	fs::RmDirAll(fs::JoinPath(config_.StoragePath, dbName));
	auth.ResetDB();

	return {};
}

std::vector<std::string> DBManager::EnumDatabases() {
	std::vector<std::string> dbs;

	shared_lock<shared_timed_mutex> lck(mtx_);
	dbs.reserve(dbs_.size());
	for (const auto &it : dbs_) dbs.emplace_back(it.first);
	return dbs;
}

void DBManager::ShutdownClusters() {
	std::unique_lock lck(mtx_);
	if (!clustersShutDown_) {
		for (auto &db : dbs_) {
			db.second->ShutdownCluster();
		}
		clustersShutDown_ = true;
	}
}

Error DBManager::Login(const std::string &dbName, AuthContext &auth) {
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

	auto it = users_.find(auth.login_);
	if (it == users_.end()) {
		return Error(errForbidden, "Unauthorized");
	}
	// TODO change to SCRAM-RSA
	if (!it->second.salt.empty()) {
		if (it->second.hash != reindexer::MD5crypt(auth.password_, it->second.salt)) {
			return Error(errForbidden, "Unauthorized");
		}
	} else if (it->second.hash != auth.password_) {
		return Error(errForbidden, "Unauthorized");
	}

	auth.role_ = kRoleNone;

	if (!dbName.empty()) {
		const UserRecord &urec = it->second;

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
	// logPrintf(LogInfo, "Authorized user '%s', to db '%s', role=%s", auth.login_, dbName, UserRoleName(auth.role_));

	return {};
}

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
	std::string content;
	int res = fs::ReadFile(fs::JoinPath(config_.StoragePath, kUsersYAMLFilename), content);
	if (res < 0) return Error(errNotFound, "Can't read '%s' file", kUsersYAMLFilename);
	try {
		YAML::ScannerOpts opts;
		opts.disableAnchors = true;
		YAML::Node root = YAML::Load(content, opts);
		for (const auto &user : root) {
			UserRecord urec;
			urec.login = user.first.as<std::string>();
			auto userNode = user.second;
			auto err = ParseMd5CryptString(userNode["hash"].as<std::string>(), urec.hash, urec.salt);
			if (!err.ok()) {
				logPrintf(LogWarning, "Hash parsing error for user '%s': %s", urec.login, err.what());
				continue;
			}
			auto userRoles = userNode["roles"];
			if (userRoles.IsMap()) {
				for (const auto &role : userRoles) {
					std::string db(role.first.as<std::string>());
					try {
						urec.roles.emplace(db, userRoleFromString(role.second.as<std::string>()));
					} catch (const Error &err) {
						logPrintf(LogWarning, "Skipping user '%s' for db '%s': ", urec.login, db, err.what());
					}
				}
				if (urec.roles.empty()) {
					logPrintf(LogWarning, "User '%s' doesn't have valid roles", urec.login);
				} else {
					users_.emplace(urec.login, urec);
				}
			} else {
				logPrintf(LogWarning, "Skipping user '%s': no 'roles' node found", urec.login);
			}
		}
	} catch (const YAML::Exception &ex) {
		return Error(errParseYAML, "Users: %s", ex.what());
	}
	return errOK;
}

Error DBManager::readUsersJSON() noexcept {
	std::string content;
	int res = fs::ReadFile(fs::JoinPath(config_.StoragePath, kUsersJSONFilename), content);
	if (res < 0) return Error(errNotFound, "Can't read '%s' file", kUsersJSONFilename);

	try {
		gason::JsonParser parser;
		auto root = parser.Parse(giftStr(content));
		for (auto &userNode : root) {
			UserRecord urec;
			urec.login = std::string(userNode.key);
			auto err = ParseMd5CryptString(userNode["hash"].As<std::string>(), urec.hash, urec.salt);
			if (!err.ok()) {
				logPrintf(LogWarning, "Hash parsing error for user '%s': %s", urec.login, err.what());
				continue;
			}
			for (auto &roleNode : userNode["roles"]) {
				std::string db(roleNode.key);
				try {
					UserRole role = userRoleFromString(roleNode.As<std::string_view>());
					urec.roles.emplace(db, role);
				} catch (const Error &err) {
					logPrintf(LogWarning, "Skipping user '%s' for db '%s': ", urec.login, db, err.what());
				}
			}
			if (urec.roles.empty()) {
				logPrintf(LogWarning, "User '%s' doesn't have valid roles", urec.login);
			} else {
				users_.emplace(urec.login, urec);
			}
		}
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "Users: %s", ex.what());
	}
	return errOK;
}

Error DBManager::createDefaultUsersYAML() noexcept {
	logPrintf(LogInfo, "Creating default %s file", kUsersYAMLFilename);
	int64_t res = fs::WriteFile(fs::JoinPath(config_.StoragePath, kUsersYAMLFilename),
								"# List of db's users, their's roles and privileges\n\n"
								"# Anchors and aliases do not work here due to compatibility reasons\n\n"
								"# Username\n"
								"reindexer:\n"
								"  # Hash type(right now '$1' is the only value), salt and hash in BSD MD5 Crypt format\n"
								"  # Hash may be generated via openssl tool - `openssl passwd -1 -salt MySalt MyPassword`\n"
								"  # If hash doesn't start with '$' sign it will be used as raw password itself\n"
								"  hash: $1$rdxsalt$VIR.dzIB8pasIdmyVGV0E/\n"
								"  # User's roles for specific databases, * in place of db name means any database\n"
								"  # Allowed roles:\n"
								"  # 1) data_read - user can read data from database\n"
								"  # 2) data_write - user can write data to database\n"
								"  # 3) db_admin - user can manage database: kRoleDataWrite + create & delete namespaces, modify indexes\n"
								"  # 4) owner - user has all privilegies on database: kRoleDBAdmin + create & drop database\n"
								"  roles:\n"
								"    *: owner\n");
	if (res < 0) {
		return Error(errParams, "Unable to write default config file: %s", strerror(errno));
	}
	users_.emplace("reindexer", UserRecord{"reindexer", "VIR.dzIB8pasIdmyVGV0E/", "rdxsalt", {{"*", kRoleOwner}}});
	return errOK;
}

UserRole DBManager::userRoleFromString(std::string_view strRole) {
	if (strRole == "data_read"sv) {
		return kRoleDataRead;
	} else if (strRole == "data_write"sv) {
		return kRoleDataWrite;
	} else if (strRole == "db_admin"sv) {
		return kRoleDBAdmin;
	} else if (strRole == "owner"sv) {
		return kRoleOwner;
	}
	throw Error(errParams, "Role \'%s\' is invalid", strRole);
}

std::string_view UserRoleName(UserRole role) noexcept {
	switch (role) {
		case kUnauthorized:
			return "unauthoried"sv;
		case kRoleNone:
			return "none"sv;
		case kRoleDataRead:
			return "data_read"sv;
		case kRoleDataWrite:
			return "data_write"sv;
		case kRoleDBAdmin:
			return "db_admin"sv;
		case kRoleOwner:
			return "owner"sv;
		case kRoleSystem:
			return "system"sv;
	}
	return ""sv;
}

}  // namespace reindexer_server
