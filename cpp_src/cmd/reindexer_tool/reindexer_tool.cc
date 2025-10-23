#include <csignal>
#include "args/args.hpp"
#include "client/reindexer.h"
#include "commandsprocessor.h"
#include "convert_tool.h"
#include "core/reindexer.h"
#include "debug/backtrace.h"
#include "reindexer_version.h"
#include "repair_tool.h"
#include "tools/cpucheck.h"
#include "tools/logger.h"

namespace reindexer_tool {

#ifdef REINDEX_WITH_LEVELDB
const bool kLevelDBAvailable = true;
#else	// REINDEX_WITH_LEVELDB
const bool kLevelDBAvailable = false;
#endif	// REINDEX_WITH_LEVELDB

#ifdef REINDEX_WITH_ROCKSDB
const bool kRocksDBAvailable = true;
#else	// REINDEX_WITH_ROCKSDB
const bool kRocksDBAvailable = false;
#endif	// REINDEX_WITH_ROCKSDB

using args::Options;

constexpr int kSingleThreadCoroCount = 200;
static int llevel = 0;

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
		[](int level, const char* buf) {
			if (level <= llevel) {
				std::cout << buf << std::endl;
			}
		},
		reindexer::LoggerPolicy::WithoutLocks, llevel);
}

}  // namespace reindexer_tool

// NOLINTNEXTLINE (bugprone-exception-escape) Get stacktrace is probably better, than generic error-message
int main(int argc, char* argv[]) {
	using namespace reindexer_tool;
	reindexer::debug::backtrace_init();

	try {
		reindexer::CheckRequiredSSESupport();
	} catch (std::exception& err) {
		std::cerr << err.what();
		return EXIT_FAILURE;
	}

	struct [[nodiscard]] Version : std::runtime_error {
		using std::runtime_error::runtime_error;
	};

	args::ArgumentParser parser("Reindexer client tool");
	args::HelpFlag help(parser, "help", "show this message", {'h', "help"});
	args::ActionFlag version(parser, "", "Reindexer tool version", {'v', "version"},
							 []() { throw Version(fmt::format("Reindexer tool version: {}", REINDEX_VERSION)); });

	args::Group progOptions("options");
#ifdef _WIN32
	args::ValueFlag<std::string> dbDsn(progOptions, "DSN",
									   "DSN to 'reindexer'. Can be 'cproto://[user@password:]<ip>:<port>/<dbname>' or 'builtin://<path>'",
									   {'d', "dsn"}, "", Options::Single | Options::Global);
#else	// _WIN32
	args::ValueFlag<std::string> dbDsn(progOptions, "DSN",
									   "DSN to 'reindexer'. Can be 'cproto://[user@password:]<ip>:<port>/<dbname>', "
									   "'cprotos://[user@password:]<ip>:<port>/<dbname>', 'builtin://<path>' or "
									   "'ucproto://[user@password:]<unix/socket/path>:/<dbname>'",
									   {'d', "dsn"}, "", Options::Single | Options::Global);
#endif	// _WIN32
	args::ValueFlag<std::string> fileName(progOptions, "FILENAME", "Execute commands from file, then exit", {'f', "filename"}, "",
										  Options::Single | Options::Global);
	args::ValueFlag<std::string> command(progOptions, "COMMAND", "Run single command (SQL or internal) and exit", {'c', "command"}, "",
										 Options::Single | Options::Global);
	args::ValueFlag<std::string> outFileName(progOptions, "FILENAME", "Send query results to file", {'o', "output"}, "",
											 Options::Single | Options::Global);

	args::ValueFlag<std::string> dumpMode(progOptions, "DUMP_MODE",
										  "Dump mode for sharded databases: 'full_node' (default), 'sharded_only', 'local_only'",
										  {"dump-mode"}, "", Options::Single | Options::Global);

	args::ValueFlag<unsigned> connThreads(progOptions, "INT=1..65535", "Number of threads(connections) used by db connector",
										  {'t', "threads"}, 4, Options::Single | Options::Global);

	args::ValueFlag<unsigned> maxTransactionSize(progOptions, "INT=1..100000",
												 "Max transaction size used by db connector(0 - no transactions)", {"txs", "txsize"}, 0,
												 Options::Single | Options::Global);

	args::Flag createDBF(progOptions, "", "Creates target database if it is missing", {"createdb"});

	args::Positional<std::string> dbName(progOptions, "DB name", "Name of a database to get connected to", Options::Single);

	args::ActionFlag logLevel(progOptions, "INT=1..5", "Reindexer logging level", {'l', "log"}, 1, &InstallLogLevel,
							  Options::Single | Options::Global);

	args::Flag repair(progOptions, "", "Try to repair storage", {'r', "repair"});

	std::string availableFormats;
	if (kLevelDBAvailable) {
		availableFormats = reindexer::datastorage::kLevelDBName;
	}
	if (kRocksDBAvailable) {
		if (!availableFormats.empty()) {
			availableFormats += ", ";
		}
		availableFormats += reindexer::datastorage::kRocksDBName;
	}

	args::ValueFlag<std::string> convertFormat(
		progOptions, "FORMAT", std::string("Try to convert storage into selected format (available formats: ") + availableFormats + ")",
		{"convfmt"});

	args::ValueFlag<std::string> convertBackupFolder(progOptions, "FOLDER", "folder to backup original data before converting",
													 {"convbackup"});

	args::ValueFlag<std::string> appName(progOptions, "Application name", "Application name that will be used in login info",
										 {'a', "appname"}, "reindexer_tool", Options::Single | Options::Global);

	args::ValueFlagList<std::string> selectedNamespaces(progOptions, "Namespaces selected",
														"List of namespaces for which commands from file will be executed. Selecting all "
														"namespaces if list is empty. Commands for non-selected namespaces will be skipped."
														"\nExample: \"reindexer_tool -n collections -n karaoke_genres\""
														"\nwill execute commands only for collections and karaoke_genres namespaces",
														{'n', "namespaces"}, {});

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
	} catch (Version& v) {
		std::cout << v.what() << std::endl;
		return 0;
	} catch (reindexer::Error& re) {
		std::cerr << "ERROR: " << re.what() << std::endl;
		return 1;
	}

	std::string dsn = args::get(dbDsn);
	Error err;
	Error runError;
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

		if (checkIfStartsWithCS("cproto://"sv, db) || checkIfStartsWithCS("ucproto://"sv, db) || checkIfStartsWithCS("cprotos://"sv, db) ||
			checkIfStartsWithCS("builtin://"sv, db)) {
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

	std::string convFmt = args::get(convertFormat);
	std::string convBackup = args::get(convertBackupFolder);
	if (!convFmt.empty()) {
		if (!kRocksDBAvailable) {
			std::cerr << "Error! Can't convert, rocksdb is not available in current build." << std::endl;
			return 1;
		}

		if (!kLevelDBAvailable) {
			std::cerr << "Error! Can't convert, leveldb is not available in current build." << std::endl;
			return 1;
		}

		err = ConvertTool::ConvertStorage(dsn, convFmt, convBackup);
		if (!err.ok()) {
			std::cerr << err.what() << std::endl;
			return 1;
		}
		return 0;
	}

	if (!convBackup.empty()) {
		std::cerr << "convbackup option can not be used without correct convfmt" << std::endl;
		return 1;
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

	if (checkIfStartsWithCS("cproto://"sv, dsn) || checkIfStartsWithCS("cprotos://"sv, dsn) || checkIfStartsWithCS("ucproto://"sv, dsn)) {
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
		config.SyncRxCoroCount = kSingleThreadCoroCount;

		const unsigned int numThreads = args::get(connThreads);
		const unsigned int numConnects = numThreads;
		const unsigned int transactionSize = args::get(maxTransactionSize);
		CommandsProcessor<reindexer::client::Reindexer> commandsProcessor(args::get(outFileName), args::get(fileName),
																		  args::get(selectedNamespaces), args::get(connThreads),
																		  transactionSize, config, numConnects, numThreads);
		err = commandsProcessor.Connect(dsn, reindexer::client::ConnectOpts().CreateDBIfMissing(createDBF && args::get(createDBF)));
		if (err.ok()) {
			runError = commandsProcessor.Run(args::get(command), args::get(dumpMode));
		}
	} else if (checkIfStartsWithCS("builtin://"sv, dsn)) {
		CommandsProcessor<reindexer::Reindexer> commandsProcessor(args::get(outFileName), args::get(fileName),
																  args::get(selectedNamespaces), args::get(connThreads), 0);
		err = commandsProcessor.Connect(dsn, ConnectOpts().DisableReplication());
		if (err.ok()) {
			runError = commandsProcessor.Run(args::get(command), args::get(dumpMode));
		}
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

	return runError.ok() ? 0 : 2;
}
