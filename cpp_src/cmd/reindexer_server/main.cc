#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include "args/args.hpp"
#include "core/reindexer.h"
#include "debug/backtrace.h"
#include "httpserver.h"
#include "rpcserver.h"
#include "time/fast_time.h"
#include "tools/fsops.h"
#include "tools/logger.h"

int logLevel;

#define STR_EXPAND(tok) #tok
#define STR(tok) STR_EXPAND(tok)

void changeUser(const char *userName) {
	struct passwd pwd, *result;
	char buf[0x4000];

	int res = getpwnam_r(userName, &pwd, buf, sizeof(buf), &result);
	if (result == nullptr) {
		if (res == 0)
			fprintf(stderr, "User %s not found\n", userName);
		else {
			errno = res;
			perror("getpwnam_r");
		}
		exit(1);
	}

	if (setgid(pwd.pw_gid) != 0) {
		fprintf(stderr, "Can't change user to %s\n", userName);
		exit(1);
	}
	if (setuid(pwd.pw_uid) != 0) {
		fprintf(stderr, "Can't change user to %s\n", userName);
		exit(1);
	}
}

int main(int argc, char **argv) {
	backtrace_init();
	string execFile = string(argv[0]);
	size_t lastSlashPos = execFile.find_last_of('/');
	string execPath = execFile.substr(0, lastSlashPos + 1);

	args::ArgumentParser parser("reindexer server");
	args::HelpFlag help(parser, "help", "show this message", {'h', "help"});
	args::ValueFlag<string> user(parser, "USER", "System user name", {'u', "user"}, "", args::Options::Single);

	args::Group dbGroup(parser, "Database options");
	args::ValueFlag<string> storage(dbGroup, "PATH", "path to 'reindexer' storage", {'s', "db"}, "/tmp/reindex", args::Options::Single);
	args::Flag noAutoOpen(dbGroup, "", "do not open all available namespaces on start", {'n', "no-auto-open"});

	args::Group netGroup(parser, "Network options");
	args::ValueFlag<int> p(netGroup, "PORT", "http listen port", {'p', "httpport"}, 8000, args::Options::Single);
	args::ValueFlag<int> rp(netGroup, "RPORT", "RPC listen port", {'r', "rpcport"}, 6534, args::Options::Single);
	args::ValueFlag<string> w(netGroup, "PATH", "web root", {'w', "webroot"}, execPath, args::Options::Single);

	args::Group logGroup(parser, "Logging options");
	args::ValueFlag<int> log(logGroup, "INT", "log level (MAX = 5)", {'l', "loglevel"}, 3, args::Options::Single);

	try {
		parser.ParseCLI(argc, argv);

	} catch (args::Help) {
		std::cout << parser;
		return 0;
	} catch (args::Error &e) {
		std::cerr << e.what() << std::endl << parser;
		return 1;
	} catch (reindexer::Error &re) {
		std::cerr << re.what() << std::endl;
		return 1;
	}

	setvbuf(stdout, 0, _IONBF, 0);
	setvbuf(stderr, 0, _IONBF, 0);

	logLevel = args::get(log);
	auto storagePath = args::get(storage);
	auto port = args::get(p);
	auto rpcPort = args::get(rp);
	auto noOpen = args::get(noAutoOpen);
	auto webRoot = args::get(w);
	auto userName = args::get(user);

	if (!userName.empty()) {
		changeUser(userName.c_str());
	}

	auto db = std::make_shared<reindexer::Reindexer>();

	auto status = db->EnableStorage(storagePath);
	if (!status.ok()) {
		fprintf(stderr, "%s\n", status.what().c_str());
		return -1;
	}

	reindexer::logInstallWriter([](int level, char *buf) {
		if (level <= logLevel) {
			std::tm tm;
			std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
			fast_gmtime_r(&t, &tm);
			char lc = 'I';
			switch (level) {
				case LogError:
					lc = 'E';
					break;
				case LogWarning:
					lc = 'W';
					break;
				case LogTrace:
				case LogInfo:
				default:
					lc = 'I';
					break;
			}

			printf("%c:[%02d.%02d.%04d %02d:%02d:%02d] %s\n", lc, tm.tm_mday, tm.tm_mon + 1, tm.tm_year + 1900, tm.tm_hour, tm.tm_min,
				   tm.tm_sec, buf);
		}
	});

	if (!noOpen) {
		vector<reindexer::DirEntry> foundNs;
		if (reindexer::ReadDir(storagePath, foundNs) < 0) {
			perror("Can't read database dir");
		}
		for (auto &de : foundNs) {
			if (de.isDir) {
				auto status = db->OpenNamespace(de.name, StorageOpts().Enabled().CreateIfMissing());
				if (!status.ok()) {
					reindexer::logPrintf(LogError, "Failed to open namespace '%s' - %s", de.name.c_str(), status.what().c_str());
				}
			}
		}
	}

	//	db->OpenNamespace("test_item");
	//	db->AddIndex("test_item", {"id", "id", "hash", "int", IndexOpts(false, true, false)});
	//	db->AddIndex("test_item", {"randomNumber", "randomNumber", "tree", "int", IndexOpts(false, false, false)});
	//	db->AddIndex("test_item",
	//				 {"caseInsensitiveStr", "caseInsensitiveStr", "tree", "string", IndexOpts(false, false, false, false, CollateASCII)});

	//	for (int i = 0; i < 10000; i++) {
	//		char buf[1024];
	//		sprintf(buf, "{\"id\":%d, \"randomNumber\": %d }", i, int(random() % 10000));
	//		std::unique_ptr<reindexer::Item> it(db->NewItem("test_item"));
	//		it->FromJSON(reindexer::Slice(buf, strlen(buf)));
	//		db->Upsert("test_item", it.get());
	//	}

	printf("Starting reindexer_server (%s) on %d HTTP port, %d RPC port, with db '%s'\n", STR(REINDEX_VERSION), port, rpcPort,
		   storagePath.c_str());

	reindexer_server::HTTPServer httpServer(db, webRoot);
	reindexer_server::RPCServer rpcServer(db);

	std::thread th([&]() {
		if (!rpcServer.Start(rpcPort)) {
			fprintf(stderr, "Can't start listen RPC on '%d'\n", port);
			exit(-1);
		}
	});
	th.detach();

	if (!httpServer.Start(port)) {
		fprintf(stderr, "Can't start listen HTTP on '%d'\n", port);
		exit(-1);
	}

	return 0;
}
