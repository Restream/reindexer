#include <csignal>
#include <limits>
#include "args/args.hpp"
#include "client/reindexer.h"
#include "commandsprocessor.h"
#include "core/reindexer.h"
#include "core/storage/storagefactory.h"
#include "debug/backtrace.h"
#include "reindexer_version.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

namespace reindexer_tool {

using args::Options;

int llevel;

const char* const kStoragePlaceholderFilename = ".reindexer.storage";

void InstallLogLevel(const vector<string>& args) {
	try {
		llevel = stoi(args.back());
		if ((llevel < 1) || (llevel > 5)) {
			throw std::out_of_range("value must be in range 1..5");
		}
	} catch (std::invalid_argument&) {
		throw args::UsageError("Value must be integer.");
	} catch (std::out_of_range& exc) {
		std::cout << "WARNING: " << exc.what() << std::endl << "Logging level set to 3" << std::endl;
		llevel = 3;
	}

	reindexer::logInstallWriter([](int level, char* buf) {
		if (level <= llevel) {
			std::cout << buf << std::endl;
		}
	});
}

int RepairStorage(const std::string& dsn) {
	if (dsn.compare(0, 10, "builtin://") != 0) {
		std::cerr << "Invalid DSN format for repair: " << dsn << " Must begin from builtin://" << std::endl;
		return 1;
	}

	std::cout << "Starting databases repair..." << std::endl;
	auto path = dsn.substr(10);
	vector<reindexer::fs::DirEntry> foundDb;
	if (reindexer::fs::ReadDir(path, foundDb) < 0) {
		std::cerr << "Can't read dir to repair: " << path << std::endl;
		return 1;
	}

	bool hasErrors = false;
	for (auto& de : foundDb) {
		if (de.isDir && reindexer::validateObjectName(de.name)) {
			auto dePath = reindexer::fs::JoinPath(path, de.name);
			auto storageType = reindexer::datastorage::StorageType::LevelDB;
			std::string content;
			int res = reindexer::fs::ReadFile(reindexer::fs::JoinPath(dePath, kStoragePlaceholderFilename), content);
			if (res > 0) {
				std::unique_ptr<reindexer::datastorage::IDataStorage> storage;
				try {
					storageType = reindexer::datastorage::StorageTypeFromString(content);
				} catch (const Error&) {
					std::cerr << "Skiping DB at \"" << dePath << "\" - it has unexpected storage type: \"" << content << "\"";
					continue;
				}
				try {
					storage.reset(reindexer::datastorage::StorageFactory::create(storageType));
				} catch (std::exception& ex) {
					std::cerr << ex.what();
					return 1;
				}
				vector<reindexer::fs::DirEntry> foundNs;
				if (reindexer::fs::ReadDir(dePath, foundNs) < 0) {
					std::cerr << "Can't read dir to repair: " << dePath << std::endl;
					continue;
				}
				for (auto& ns : foundNs) {
					if (ns.isDir && reindexer::validateObjectName(de.name)) {
						auto nsPath = reindexer::fs::JoinPath(dePath, ns.name);
						std::cout << "Repairing " << nsPath << "..." << std::endl;
						auto err = storage->Repair(nsPath);
						if (!err.ok()) {
							hasErrors = true;
							std::cerr << "Repair error [" << nsPath << "]: " << err.what() << std::endl;
						}
					}
				}
			} else {
				std::cerr << "Skiping DB at \"" << dePath << "\" - directory doesn't contain valid reindexer placeholder";
				continue;
			}
		}
	}

	return hasErrors ? 2 : 0;
}

}  // namespace reindexer_tool

int main(int argc, char* argv[]) {
	using namespace reindexer_tool;
	reindexer::debug::backtrace_init();

	args::ArgumentParser parser("Reindexer client tool");
	args::HelpFlag help(parser, "help", "show this message", {'h', "help"});

	args::Group progOptions("options");
	args::ValueFlag<string> dbDsn(progOptions, "DSN", "DSN to 'reindexer'. Can be 'cproto://<ip>:<port>/<dbname>' or 'builtin://<path>'",
								  {'d', "dsn"}, "", Options::Single | Options::Global);
	args::ValueFlag<string> fileName(progOptions, "FILENAME", "execute commands from file, then exit", {'f', "filename"}, "",
									 Options::Single | Options::Global);
	args::ValueFlag<string> command(progOptions, "COMMAND", "run only single command (SQL or internal) and exit'", {'c', "command"}, "",
									Options::Single | Options::Global);
	args::ValueFlag<string> outFileName(progOptions, "FILENAME", "send query results to file", {'o', "output"}, "",
										Options::Single | Options::Global);
	args::ValueFlag<int> connPoolSize(progOptions, "INT", "Number of simulateonous connections to db", {'C', "connections"}, 1,
									  Options::Single | Options::Global);

	args::ValueFlag<int> connThreads(progOptions, "INT", "Number of threads used by db connector", {'t', "threads"}, 1,
									 Options::Single | Options::Global);

	args::Positional<string> dbName(progOptions, "DB name", "Name of a database to get connected to", Options::Single);

	args::ActionFlag logLevel(progOptions, "INT=1..5", "reindexer logging level", {'l', "log"}, 1, &InstallLogLevel,
							  Options::Single | Options::Global);

	args::Flag repair(progOptions, "", "Repair database", {'r', "repair"});

	args::GlobalOptions globals(parser, progOptions);

	try {
		parser.ParseCLI(argc, argv);
	} catch (const args::Help&) {
		std::cout << parser;
	} catch (const args::Error& e) {
		std::cerr << "ERROR: " << e.what() << std::endl;
		std::cout << parser.Help() << std::endl;
		return 1;
	} catch (reindexer::Error& re) {
		std::cerr << "ERROR: " << re.what() << std::endl;
		return 1;
	}

	string dsn = args::get(dbDsn);
	bool ok = false;
	Error err;
#ifndef _WIN32
	signal(SIGPIPE, SIG_IGN);
#endif

	string db;
	if (dsn.empty()) {
		db = args::get(dbName);
		if (db.empty()) {
			std::cerr << "Error: --dsn either database name should be set as a first argument" << std::endl;
			return 2;
		}
		dsn = "cproto://127.0.0.1:6534/" + db;
	}

	if (repair && args::get(repair)) {
		return RepairStorage(dsn);
	}

	if (!args::get(command).length() && !args::get(fileName).length()) {
		std::cout << "Reindexer command line tool version " << REINDEX_VERSION << std::endl;
	}

	reindexer::client::ReindexerConfig config;
	config.ConnPoolSize = args::get(connPoolSize);
	config.WorkerThreads = 1;  // args::get(connThreads);
	if (dsn.compare(0, 9, "cproto://") == 0) {
		CommandsProcessor<reindexer::client::Reindexer> commandsProcessor(args::get(outFileName), args::get(fileName), args::get(command),
																		  config.ConnPoolSize, args::get(connThreads), config);
		err = commandsProcessor.Connect(dsn);
		if (err.ok()) ok = commandsProcessor.Run();
	} else if (dsn.compare(0, 10, "builtin://") == 0) {
		reindexer::Reindexer db;
		CommandsProcessor<reindexer::Reindexer> commandsProcessor(args::get(outFileName), args::get(fileName), args::get(command),
																  config.ConnPoolSize, args::get(connThreads));
		err = commandsProcessor.Connect(dsn);
		if (err.ok()) ok = commandsProcessor.Run();
	} else {
		std::cerr << "Invalid DSN format: " << dsn << " Must begin from cproto:// or builtin://" << std::endl;
	}
	if (!err.ok()) {
		std::cerr << "ERROR: " << err.what() << std::endl;
	}

	return ok ? 0 : 2;
}
