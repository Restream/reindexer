#include <fstream>
#include <mutex>

#include <thread>
#include "dbmanager.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

namespace reindexer_server {
DBManager::DBManager(const string &dbpath, bool noSecurity)
	: dbpath_(dbpath), noSecurity_(noSecurity), storageType_(datastorage::StorageType::LevelDB) {}

Error DBManager::Init(const std::string &storageEngine, bool allowDBErrors) {
	auto status = readUsers();
	if (!status.ok() && !noSecurity_) {
		return status;
	}

	vector<fs::DirEntry> foundDb;
	if (fs::ReadDir(dbpath_, foundDb) < 0) {
		return Error(errParams, "Can't read reindexer dir %s", dbpath_);
	}

	try {
		storageType_ = datastorage::StorageTypeFromString(storageEngine);
	} catch (const Error &err) {
		return err;
	}

	for (auto &de : foundDb) {
		if (de.isDir && validateObjectName(de.name)) {
			auto status = loadOrCreateDatabase(de.name, allowDBErrors);
			if (!status.ok()) {
				logPrintf(LogError, "Failed to open database '%s' - %s", de.name, status.what());
				if (status.code() == errNotValid) {
					logPrintf(LogError, "Try to run:\n\treindexer_tool --dsn \"builtin://%s\" --repair\nto restore data", dbpath_);
					return status;
				}
			}
		}
	}

	return 0;
}

Error DBManager::OpenDatabase(const string &dbName, AuthContext &auth, bool canCreate) {
	RdxContext dummyCtx;
	auto status = Login(dbName, auth);
	if (!status.ok()) {
		return status;
	}

	smart_lock<Mutex> lck(mtx_, dummyCtx);
	auto it = dbs_.find(dbName);
	if (it != dbs_.end()) {
		auth.db_ = it->second.get();
		return 0;
	}
	lck.unlock();

	if (!canCreate) {
		return Error(errParams, "Database '%s' not found", dbName);
	}
	if (auth.role_ < kRoleOwner) {
		return Error(errForbidden, "Forbidden to create database %s", dbName);
	}
	if (!validateObjectName(dbName)) {
		return Error(errParams, "Database name contains invalid character. Only alphas, digits,'_','-, are allowed");
	}

	lck = smart_lock<Mutex>(mtx_, dummyCtx, true);
	it = dbs_.find(dbName);
	if (it != dbs_.end()) {
		auth.db_ = it->second.get();
		return 0;
	}

	status = loadOrCreateDatabase(dbName, true);
	if (!status.ok()) {
		return status;
	}

	lck.unlock();
	return OpenDatabase(dbName, auth, false);
}

Error DBManager::loadOrCreateDatabase(const string &dbName, bool allowDBErrors) {
	string storagePath = fs::JoinPath(dbpath_, dbName);

	logPrintf(LogInfo, "Loading database %s", dbName);
	auto db = unique_ptr<reindexer::Reindexer>(new reindexer::Reindexer);
	StorageTypeOpt storageType = kStorageTypeOptLevelDB;
	switch (storageType_) {
		case datastorage::StorageType::LevelDB:
			storageType = kStorageTypeOptLevelDB;
			break;
		case datastorage::StorageType::RocksDB:
			storageType = kStorageTypeOptRocksDB;
			break;
	}
	auto status = db->Connect(storagePath, ConnectOpts().AllowNamespaceErrors(allowDBErrors).WithStorageType(storageType));
	if (status.ok()) {
		dbs_[dbName] = std::move(db);
	}

	return status;
}

void DBManager::collectStats() noexcept {}

Error DBManager::DropDatabase(AuthContext &auth) {
	{
		Reindexer *db = nullptr;
		auto status = auth.GetDB(kRoleOwner, &db);
		if (!status.ok()) {
			return status;
		}
	}
	string dbName = auth.dbName_;

	std::unique_lock<shared_timed_mutex> lck(mtx_);
	auto it = dbs_.find(auth.dbName_);
	if (it == dbs_.end()) {
		return Error(errParams, "Database %s not found", dbName);
	}
	dbs_.erase(it);
	auth.ResetDB();

	fs::RmDirAll(fs::JoinPath(dbpath_, dbName));
	return 0;
}

vector<string> DBManager::EnumDatabases() {
	shared_lock<shared_timed_mutex> lck(mtx_);
	vector<string> dbs;
	for (auto &it : dbs_) dbs.push_back(it.first);
	return dbs;
}

Error DBManager::Login(const string &dbName, AuthContext &auth) {
	if (kRoleSystem == auth.role_) {
		auth.dbName_ = dbName;
		return 0;
	}

	if (IsNoSecurity()) {
		auth.role_ = kRoleOwner;
		auth.dbName_ = dbName;
		return 0;
	}

	if (auth.role_ != kUnauthorized && dbName == auth.dbName_) {
		return 0;
	}

	auto it = users_.find(auth.login_);
	if (it == users_.end()) {
		return Error(errForbidden, "Unauthorized");
	}
	// TODO change to SCRAM-RSA
	if (it->second.hash != auth.password_) {
		return Error(errForbidden, "Unauthorized");
	}

	auth.role_ = kRoleNone;

	if (!dbName.empty()) {
		const UserRecord &urec = it->second;
		auth.role_ = kRoleNone;

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
	logPrintf(LogInfo, "Authorized user '%s', to db '%s', role=%s", auth.login_, dbName, UserRoleName(auth.role_));

	return 0;
}

Error DBManager::readUsers() {
	users_.clear();
	string content;
	int res = fs::ReadFile(fs::JoinPath(dbpath_, "users.json"), content);
	if (res < 0) return Error(errParams, "Can't read users.json file");

	try {
		gason::JsonParser parser;
		auto root = parser.Parse(giftStr(content));

		for (auto &userNode : root) {
			UserRecord urec;
			urec.login = string(userNode.key);
			urec.hash = userNode["hash"].As<string>();
			for (auto &roleNode : userNode["roles"]) {
				string db(roleNode.key);
				UserRole role = kRoleDataRead;
				string_view strRole = roleNode.As<string_view>();
				if (strRole == "data_read"_sv) {
					role = kRoleDataRead;
				} else if (strRole == "data_write"_sv) {
					role = kRoleDataWrite;
				} else if (strRole == "db_admin"_sv) {
					role = kRoleDBAdmin;
				} else if (strRole == "owner"_sv) {
					role = kRoleOwner;
				} else {
					logPrintf(LogWarning, "Skipping invalid role '%s' of user '%s' for db '%s'", strRole, urec.login, db);
				}
				urec.roles.emplace(db, role);
			}
			users_.emplace(urec.login, urec);
		}
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "Users: %s", ex.what());
	}
	return errOK;
}

const char *UserRoleName(UserRole role) noexcept {
	switch (role) {
		case kUnauthorized:
			return "unauthoried";
		case kRoleNone:
			return "none";
		case kRoleDataRead:
			return "data_read";
		case kRoleDataWrite:
			return "data_write";
		case kRoleDBAdmin:
			return "db_admin";
		case kRoleOwner:
			return "owner";
		case kRoleSystem:
			return "system";
	}
	return "";
}

}  // namespace reindexer_server
