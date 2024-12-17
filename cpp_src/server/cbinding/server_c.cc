#include "server_c.h"
#include "core/cbinding/reindexer_c.h"
#include "core/cbinding/reindexer_wrapper.h"
#include "server/dbmanager.h"
#include "server/server.h"

using namespace reindexer_server;
using reindexer::Error;

const static Error err_not_init(errNotValid, "Reindexer server has not initialized");
static reindexer::WrapersMap wrappersMap_;

int check_server_ready(uintptr_t psvc) {
	auto svc = reinterpret_cast<Server*>(psvc);
	return svc && svc->IsReady();
}

static reindexer_error error2c(const Error& err_) {
	reindexer_error err;
	err.code = err_.code();
	err.what = err_.what().length() ? strdup(err_.what().c_str()) : nullptr;
	return err;
}

static std::string str2c(reindexer_string gs) { return std::string(reinterpret_cast<const char*>(gs.p), gs.n); }

uintptr_t init_reindexer_server() {
	reindexer_init_locale();
	Server* svc = new Server;
	return uintptr_t(svc);
}

void destroy_reindexer_server(uintptr_t psvc) {
	auto svc = reinterpret_cast<Server*>(psvc);
	wrappersMap_.erase(svc);
	delete svc;
}

reindexer_error start_reindexer_server(uintptr_t psvc, reindexer_string _config) {
	Error err = err_not_init;
	auto svc = reinterpret_cast<Server*>(psvc);
	if (svc) {
		err = svc->InitFromYAML(str2c(_config));
		if (!err.ok()) {
			return error2c(err);
		}
		err = (svc->Start() == 0 ? Error{} : Error(errLogic, "server startup error"));
	}
	return error2c(err);
}

reindexer_error get_reindexer_instance(uintptr_t psvc, reindexer_string dbname, reindexer_string user, reindexer_string pass,
									   uintptr_t* rx) {
	Reindexer* target_db = nullptr;

	Error err = err_not_init;
	auto svc = reinterpret_cast<Server*>(psvc);
	if (check_server_ready(psvc)) {
		AuthContext ctx(str2c(user), str2c(pass));
		err = svc->GetDBManager().OpenDatabase(str2c(dbname), ctx, true);
		if (err.ok()) {
			err = ctx.GetDB<AuthContext::CalledFrom::Core>(kRoleOwner, &target_db);
		}
	}

	if (err.ok()) {
		// Creating non-owning reindexer copy
		auto [ptr, emplaced] = wrappersMap_.try_emplace(svc, target_db);
		(void)emplaced;
		*rx = reinterpret_cast<uintptr_t>(ptr);
	} else {
		*rx = 0;
	}
	return error2c(err);
}

reindexer_error stop_reindexer_server(uintptr_t psvc) {
	Error err = err_not_init;
	auto svc = reinterpret_cast<Server*>(psvc);
	if (svc) {
		svc->Stop();
		err = Error(errOK);
	}
	wrappersMap_.erase(svc);
	return error2c(err);
}

reindexer_error reopen_log_files(uintptr_t psvc) {
	Error err = err_not_init;
	auto svc = reinterpret_cast<Server*>(psvc);
	if (svc) {
		svc->ReopenLogFiles();
		err = Error(errOK);
	}
	return error2c(err);
}
