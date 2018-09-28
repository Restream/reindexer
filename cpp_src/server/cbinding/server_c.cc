#include "server_c.h"
#include <stdlib.h>
#include <string.h>
#include <locale>
#include <string>
#include "server/server.h"

using namespace reindexer_server;
using std::string;
using reindexer::Error;

static shared_ptr<Server> svc;
static Error err_not_init(-1, "Reindexer server has not initialized");

struct server_access {
	static Error get_reindexer_instance(const string& dbname, const string& user, const string& password, shared_ptr<Reindexer>& rx) {
		Error err = err_not_init;
		if (check_server_ready()) {
			AuthContext ctx(user, password);
			err = svc->dbMgr_->OpenDatabase(dbname, ctx, true);
			if (err.ok()) err = ctx.GetDB(kRoleOwner, &rx);
		}
		return err;
	}

	static bool check_server_ready() { return svc && svc->storageLoaded_.load(); }
};

static reindexer_error error2c(const Error& err_) {
	reindexer_error err;
	err.code = err_.code();
	err.what = err_.what().length() ? strdup(err_.what().c_str()) : nullptr;
	return err;
}

static string str2c(reindexer_string gs) { return string(reinterpret_cast<const char*>(gs.p), gs.n); }

void init_reindexer_server() {
	if (svc) return;
	svc = std::make_shared<Server>();
	setvbuf(stdout, nullptr, _IONBF, 0);
	setvbuf(stderr, nullptr, _IONBF, 0);
	setlocale(LC_CTYPE, "");
	setlocale(LC_NUMERIC, "C");
}

void destroy_reindexer_server() { svc.reset(); }

reindexer_error start_reindexer_server(reindexer_string _config) {
	Error err = err_not_init;
	if (svc) {
		err = svc->InitFromYAML(str2c(_config));
		if (!err.ok()) return error2c(err);
		err = (svc->Start() == 0 ? 0 : Error(errLogic, "server startup error"));
	}
	return error2c(err);
}

int check_server_ready() { return server_access::check_server_ready() ? 1 : 0; }

reindexer_error get_reindexer_instance(reindexer_string _dbname, reindexer_string _user, reindexer_string _pass, uintptr_t* rx) {
	shared_ptr<Reindexer> target_db;
	Error err = server_access::get_reindexer_instance(str2c(_dbname), str2c(_user), str2c(_pass), target_db);
	*rx = err.ok() ? reinterpret_cast<uintptr_t>(target_db.get()) : 0;
	return error2c(err);
}

reindexer_error stop_reindexer_server() {
	Error err = err_not_init;
	if (svc) {
		svc->Stop();
		err = Error(0);
	}
	return error2c(err);
}
