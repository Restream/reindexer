#include <stdio.h>
#include "core/reindexer.h"
#include "cxxopts/cxxopts.hpp"
#include "debug/backtrace.h"
#include "server.h"
#include "tools/fsops.h"
#include "tools/logger.h"

int logLevel, port;
string storagePath;
string webRoot;
bool noOpen = false;

void parseOptions(int argc, char **argv) {
	string execFile = string(argv[0]);
	size_t lastSlashPos = execFile.find_last_of('/');
	string execPath = execFile.substr(0, lastSlashPos + 1);

	cxxopts::Options opts("reindexer_server", "");

	opts.add_options()("h,help", "show this message");

	auto dbGroup = opts.add_options("database");
	dbGroup("s,db", "path to 'reindexer' storage", cxxopts::value<string>(storagePath)->default_value("/tmp/reindex"), "DIRECTORY");
	dbGroup("n,no-auto-open", "do not open all available namespaces on start");

	auto netGroup = opts.add_options("net");
	netGroup("p,port", "listen port", cxxopts::value<int>(port)->default_value("8000"), "INT");
	netGroup("w,webroot", "web root", cxxopts::value<string>(webRoot)->default_value(execPath), "DIRECTORY");

	auto logGroup = opts.add_options("logging");
	logGroup("l,loglevel", "log level (MAX = 5)", cxxopts::value<int>(logLevel)->default_value("3"), "INT");

	try {
		opts.parse(argc, argv);
	} catch (cxxopts::OptionException &exc) {
		fprintf(stderr, "Parameters error: %s.\n\nTry '%s -h' to show available options\n", exc.what(), argv[0]);
		exit(-1);
	}
	if (opts.count("help")) {
		printf("%s", opts.help(opts.groups()).c_str());
		exit(0);
	}
	noOpen = opts.count("no-auto-open") != 0;
}

int main(int argc, char **argv) {
	backtrace_init();
	parseOptions(argc, argv);

	auto db = std::make_shared<reindexer::Reindexer>();

	auto status = db->EnableStorage(storagePath);
	if (!status.ok()) {
		fprintf(stderr, "%s\n", status.what().c_str());
		return -1;
	}

	reindexer::logInstallWriter([](int level, char *buf) {
		if (level <= logLevel) {
			fprintf(stderr, "%s\n", buf);
		}
	});

	if (!noOpen) {
		vector<reindexer::DirEntry> foundNs;
		if (reindexer::ReadDir(storagePath, foundNs) < 0) {
			perror("Can't read database dir");
		}
		for (auto &de : foundNs) {
			if (de.isDir) {
				auto status = db->OpenNamespace(reindexer::NamespaceDef(de.name, StorageOpts(true, false, false, false)));
				if (!status.ok()) {
					reindexer::logPrintf(LogError, "Failed to open namespace '%s' - %s", de.name.c_str(), status.what().c_str());
				}
			}
		}
	}

	db->OpenNamespace(
		reindexer::NamespaceDef("test_item")
			.AddIndex("id", "id", "hash", "int", IndexOpts(false, true, false))
			.AddIndex("randomNumber", "randomNumber", "tree", "int")
			.AddIndex("caseInsensitiveStr", "caseInsensitiveStr", "tree", "string", IndexOpts(false, false, false, false, CollateASCII)));

	for (int i = 0; i < 10000; i++) {
		char buf[1024];
		sprintf(buf, "{\"id\":%d, \"randomNumber\": %d }", i, (int)(random() % 10000));
		std::unique_ptr<reindexer::Item> it(db->NewItem("test_item"));
		it->FromJSON(reindexer::Slice(buf, strlen(buf)));
		db->Upsert("test_item", it.get());
	}

	reindexer::logPrintf(LogInfo, "Starting reindexer_server on %d port, with db '%s'", port, storagePath.c_str());
	reindexer_server::Server server(db, webRoot);

	if (!server.Start(port)) {
		fprintf(stderr, "Can't start listen on '%d'\n", port);
		return -1;
	}

	return 0;
}
