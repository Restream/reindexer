#include <csignal>
#include <limits>
#include "args/args.hpp"
#include "client/cororeindexer.h"
#include "commandsprocessor.h"
#include "core/reindexer.h"
#include "debug/backtrace.h"
#include "reindexer_version.h"
#include "repair_tool.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

namespace reindexer_tool {

using args::Options;

static int llevel;

static void InstallLogLevel(const vector<string>& args) {
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

	args::ValueFlag<int> connThreads(progOptions, "INT", "Number of threads(connections) used by db connector", {'t', "threads"}, 1,
									 Options::Single | Options::Global);

	args::Flag createDBF(progOptions, "", "Enable created database if missed", {"createdb"});

	args::Positional<string> dbName(progOptions, "DB name", "Name of a database to get connected to", Options::Single);

	args::ActionFlag logLevel(progOptions, "INT=1..5", "reindexer logging level", {'l', "log"}, 1, &InstallLogLevel,
							  Options::Single | Options::Global);

	args::Flag repair(progOptions, "", "Repair database", {'r', "repair"});

	args::GlobalOptions globals(parser, progOptions);

	args::ValueFlag<string> appName(progOptions, "Application name", "Application name which will be used in login info", {'a', "appname"},
									"reindexer_tool", Options::Single | Options::Global);

	try {
		parser.ParseCLI(argc, argv);
	} catch (const args::Help&) {
		std::cout << parser;
		return 2;
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
		if (db.substr(0, 9) == "cproto://" || db.substr(0, 10) == "builtin://") {
			dsn = db;
		} else {
			dsn = "cproto://reindexer:reindexer@127.0.0.1:6534/" + db;
		}
	}

	if (repair && args::get(repair)) {
		err = RepairTool::RepairStorage(dsn);
		if (!err.ok()) {
			std::cerr << err.what() << std::endl;
			return 1;
		}
		return 0;
	}

	if (!args::get(command).length() && !args::get(fileName).length()) {
		std::cout << "Reindexer command line tool version " << REINDEX_VERSION << std::endl;
	}

	if (dsn.compare(0, 9, "cproto://") == 0) {
		reindexer::client::CoroReindexerConfig config;
		config.EnableCompression = true;
		config.AppName = args::get(appName);
		CommandsProcessor<reindexer::client::CoroReindexer> commandsProcessor(args::get(outFileName), args::get(fileName),
																			  args::get(connThreads), config);
		err = commandsProcessor.Connect(dsn, reindexer::client::ConnectOpts().CreateDBIfMissing(createDBF && args::get(createDBF)));
		if (err.ok()) ok = commandsProcessor.Run(args::get(command));
	} else if (dsn.compare(0, 10, "builtin://") == 0) {
		reindexer::Reindexer db;
		CommandsProcessor<reindexer::Reindexer> commandsProcessor(args::get(outFileName), args::get(fileName), args::get(connThreads));
		err = commandsProcessor.Connect(dsn, ConnectOpts().DisableReplication());
		if (err.ok()) ok = commandsProcessor.Run(args::get(command));
	} else {
		std::cerr << "Invalid DSN format: " << dsn << " Must begin from cproto:// or builtin://" << std::endl;
	}
	if (!err.ok()) {
		std::cerr << "ERROR: " << err.what() << std::endl;
	}

	return ok ? 0 : 2;
}
