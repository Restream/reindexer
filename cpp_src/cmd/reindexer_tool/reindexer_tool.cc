#include <csignal>
#include <limits>
#include "args/args.hpp"
#include "client/cororeindexer.h"
#include "commandsprocessor.h"
#include "core/reindexer.h"
#include "debug/backtrace.h"
#include "reindexer_version.h"
#include "repair_tool.h"
#include "tools/cpucheck.h"
#include "tools/logger.h"

namespace reindexer_tool {

using args::Options;

static int llevel;

static void InstallLogLevel(const std::vector<std::string>& args) {
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

	reindexer::logInstallWriter(
		[](int level, char* buf) {
			if (level <= llevel) {
				std::cout << buf << std::endl;
			}
		},
		reindexer::LoggerPolicy::WithoutLocks);
}

}  // namespace reindexer_tool

int main(int argc, char* argv[]) {
	using namespace reindexer_tool;
	reindexer::debug::backtrace_init();

	try {
		reindexer::CheckRequiredSSESupport();
	} catch (Error& err) {
		std::cerr << err.what();
		return EXIT_FAILURE;
	}

	args::ArgumentParser parser("Reindexer client tool");
	args::HelpFlag help(parser, "help", "show this message", {'h', "help"});

	args::Group progOptions("options");
#ifdef _WIN32
	args::ValueFlag<std::string> dbDsn(progOptions, "DSN",
									   "DSN to 'reindexer'. Can be 'cproto://<ip>:<port>/<dbname>' or 'builtin://<path>'", {'d', "dsn"}, "",
									   Options::Single | Options::Global);
#else	// _WIN32
	args::ValueFlag<std::string> dbDsn(
		progOptions, "DSN",
		"DSN to 'reindexer'. Can be 'cproto://<ip>:<port>/<dbname>', 'builtin://<path>' or 'ucproto://<unix.socket.path>:/<dbname>'",
		{'d', "dsn"}, "", Options::Single | Options::Global);
#endif	// _WIN32
	args::ValueFlag<std::string> fileName(progOptions, "FILENAME", "execute commands from file, then exit", {'f', "filename"}, "",
									 Options::Single | Options::Global);
	args::ValueFlag<std::string> command(progOptions, "COMMAND", "run only single command (SQL or internal) and exit'", {'c', "command"}, "",
									Options::Single | Options::Global);
	args::ValueFlag<std::string> outFileName(progOptions, "FILENAME", "send query results to file", {'o', "output"}, "",
										Options::Single | Options::Global);
	args::ValueFlag<std::string> dumpMode(progOptions, "DUMP_MODE",
									 "dump mode for sharded databases: 'full_node' (default), 'sharded_only', 'local_only'", {"dump-mode"},
									 "", Options::Single | Options::Global);

	args::ValueFlag<unsigned> connThreads(progOptions, "INT=1..65535", "Number of threads(connections) used by db connector",
										  {'t', "threads"}, 1, Options::Single | Options::Global);

	args::Flag createDBF(progOptions, "", "Enable created database if missed", {"createdb"});

	args::Positional<std::string> dbName(progOptions, "DB name", "Name of a database to get connected to", Options::Single);

	args::ActionFlag logLevel(progOptions, "INT=1..5", "reindexer logging level", {'l', "log"}, 1, &InstallLogLevel,
							  Options::Single | Options::Global);

	args::Flag repair(progOptions, "", "Repair database", {'r', "repair"});

	args::ValueFlag<std::string> appName(progOptions, "Application name", "Application name which will be used in login info",
										 {'a', "appname"}, "reindexer_tool", Options::Single | Options::Global);

	args::GlobalOptions globals(parser, progOptions);

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

	std::string dsn = args::get(dbDsn);
	bool ok = false;
	Error err;
#ifndef _WIN32
	signal(SIGPIPE, SIG_IGN);
#endif

	using namespace std::string_view_literals;
	using reindexer::checkIfStartsWithCS;
	std::string db;
	if (dsn.empty()) {
		db = args::get(dbName);
		if (db.empty()) {
			std::cerr << "Error: --dsn either database name should be set as a first argument" << std::endl;
			return 2;
		}

		if (checkIfStartsWithCS("cproto://"sv, db) || checkIfStartsWithCS("ucproto://"sv, db) || checkIfStartsWithCS("builtin://"sv, db)) {
#ifdef _WIN32
			if (checkIfStartsWithCS("ucproto://"sv, db) == 0) {
				std::cerr << "Invalid DSN: ucproto:// is not supported on the Windows platform. Use cproto:// or builtin:// instead"
						  << std::endl;
				return 2;
			}
#endif	// _WIN32
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

	if (checkIfStartsWithCS("cproto://"sv, dsn) || checkIfStartsWithCS("ucproto://"sv, dsn)) {
#ifdef _WIN32
		if (checkIfStartsWithCS("ucproto://"sv, dsn)) {
			std::cerr << "Invalid DSN: ucproto:// is not supported on the Windows platform. Use cproto:// or builtin:// instead"
					  << std::endl;
			return 2;
		}
#endif	// _WIN32
		reindexer::client::ReindexerConfig config;
		config.EnableCompression = true;
		config.AppName = args::get(appName);
		CommandsProcessor<reindexer::client::CoroReindexer> commandsProcessor(args::get(outFileName), args::get(fileName),
																			  args::get(connThreads), config);
		err = commandsProcessor.Connect(dsn, reindexer::client::ConnectOpts().CreateDBIfMissing(createDBF && args::get(createDBF)));
		if (err.ok()) ok = commandsProcessor.Run(args::get(command), args::get(dumpMode));
	} else if (checkIfStartsWithCS("builtin://"sv, dsn)) {
		CommandsProcessor<reindexer::Reindexer> commandsProcessor(args::get(outFileName), args::get(fileName), args::get(connThreads));
		err = commandsProcessor.Connect(dsn, ConnectOpts().DisableReplication());
		if (err.ok()) ok = commandsProcessor.Run(args::get(command), args::get(dumpMode));
	} else {
#ifdef _WIN32
		std::cerr << "Invalid DSN format: " << dsn << " Must begin from cproto:// or builtin://" << std::endl;
#else	// _WIN32
		std::cerr << "Invalid DSN format: " << dsn << " Must begin from cproto://, ucproto:// or builtin://" << std::endl;
#endif	// _WIN32
	}
	if (!err.ok()) {
		std::cerr << "ERROR: " << err.what() << std::endl;
	}

	return ok ? 0 : 2;
}
