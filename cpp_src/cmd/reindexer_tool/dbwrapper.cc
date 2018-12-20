#include "dbwrapper.h"
#include <iomanip>
#include <thread>
#include "client/reindexer.h"
#include "core/reindexer.h"
#if REINDEX_WITH_REPLXX
#include "replxx.hxx"
#endif

#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/stringstools.h"
#include "vendor/gason/gason.h"

namespace reindexer_tool {

using reindexer::iequals;
using reindexer::WrSerializer;
using reindexer::NamespaceDef;
using reindexer::JsonBuilder;
using reindexer::Query;

const string kVariableOutput = "output";
const string kOutputModeJson = "json";
const string kOutputModePretty = "pretty";
const string kOutputModePrettyCollapsed = "collapsed";
const string kOutputModeTable = "table";
const string kBenchNamespace = "rxtool_bench";
const string kBenchIndex = "id";

const int kBenchItemsCount = 10000;
const int kBenchDefaultTime = 5;

template <typename _DB>
Error DBWrapper<_DB>::Connect(const string& dsn) {
	variables_[kVariableOutput] = kOutputModePrettyCollapsed;
	return db_.Connect(dsn);
}

template <typename _DB>
DBWrapper<_DB>::~DBWrapper() {}

template <typename _DB>
Error DBWrapper<_DB>::ProcessCommand(string command) {
	LineParser parser(command);
	auto token = parser.NextToken();

	if (!token.length() || token.substr(0, 2) == "--") return errOK;

	for (auto& c : cmds_) {
		if (iequals(token, c.command)) return (this->*(c.handler))(command);
	}
	return Error(errParams, "Unknown command '%s'. Type '\\help' to list of available commands", token.ToString().c_str());
}

template <typename _DB>
Error DBWrapper<_DB>::commandSelect(const string& command) {
	typename _DB::QueryResultsT results;
	Query q;
	try {
		q.FromSQL(command);
	} catch (const Error& err) {
		return err;
	}
	auto err = db_.Select(q, results);

	if (err.ok()) {
		if (results.Count()) {
			output_() << "[" << std::endl;
			err = queryResultsToJson(output_(), results);
			output_() << "]" << std::endl;
		}

		string explain = results.GetExplainResults();
		if (!explain.empty()) {
			output_() << "Explain: " << std::endl;
			if (variables_[kVariableOutput] == kOutputModePretty) {
				WrSerializer ser;
				prettyPrintJSON(explain, ser);
				output_() << ser.Slice() << std::endl;
			} else {
				output_() << explain << std::endl;
			}
		}
		output_() << "Returned " << results.Count() << " rows";
		if (results.TotalCount()) output_() << ", total count " << results.TotalCount();
		output_() << std::endl;

		auto& aggResults = results.GetAggregationResults();
		if (aggResults.size()) {
			output_() << "Aggregations: " << std::endl;
			for (auto& agg : aggResults) {
				if (!agg.facets.empty()) {
					int maxW = agg.field.length();
					for (auto& row : agg.facets) {
						maxW = std::max(maxW, int(row.value.length()));
					}
					maxW += 3;
					output_() << std::left << std::setw(maxW) << agg.field << "| count" << std::endl;
					output_() << std::left << std::setw(maxW + 8) << std::setfill('-') << "" << std::endl << std::setfill(' ');
					for (auto& row : agg.facets) {
						output_() << std::left << std::setw(maxW) << row.value << "| " << row.count << std::endl;
					}
				} else {
					output_() << agg.aggTypeToStr(agg.type) << "(" << agg.field << ") = " << agg.value << std::endl;
				}
			}
		}
	}
	return err;
}
template <typename _DB>
Error DBWrapper<_DB>::commandDeleteSQL(const string& command) {
	typename _DB::QueryResultsT results;
	Query q;
	try {
		q.FromSQL(command);
	} catch (const Error& err) {
		return err;
	}
	auto err = db_.Delete(q, results);

	if (err.ok()) {
		output_() << "Deleted " << results.Count() << " documents" << std::endl;
	}
	return err;
}

template <typename _DB>
Error DBWrapper<_DB>::commandUpsert(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	string nsName = unescapeName(parser.NextToken());

	auto item = new typename _DB::ItemT(db_.NewItem(nsName));

	Error status = item->Status();
	if (!status.ok()) {
		delete item;
		return status;
	}

	status = item->Unsafe().FromJSON(const_cast<char*>(parser.CurPtr()));
	if (!status.ok()) {
		delete item;
		return status;
	}

	return db_.Upsert(nsName, *item, [item](const Error& /*err*/) { delete item; });
}

template <typename _DB>
Error DBWrapper<_DB>::commandDelete(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	auto nsName = unescapeName(parser.NextToken());

	auto item = db_.NewItem(nsName);
	if (!item.Status().ok()) return item.Status();

	auto err = item.Unsafe().FromJSON(const_cast<char*>(parser.CurPtr()));
	if (!err.ok()) return err;

	return db_.Delete(nsName, item);
}

template <typename _DB>
Error DBWrapper<_DB>::commandDump(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	vector<NamespaceDef> allNsDefs, doNsDefs;

	auto err = db_.EnumNamespaces(allNsDefs, false);
	if (err) return err;

	if (!parser.End()) {
		// build list of namespaces for dumped
		while (!parser.End()) {
			auto ns = parser.NextToken();
			auto nsDef = std::find_if(allNsDefs.begin(), allNsDefs.end(), [&ns](const NamespaceDef& nsDef) { return ns == nsDef.name; });
			if (nsDef != allNsDefs.end()) {
				doNsDefs.push_back(std::move(*nsDef));
				allNsDefs.erase(nsDef);
			} else {
				std::cerr << "Namespace '" << ns << "' - skipped. (not found in storage)" << std::endl;
			}
		}
	} else {
		doNsDefs = std::move(allNsDefs);
	}

	reindexer::WrSerializer wrser;

	wrser << "-- Reindexer DB backup file" << '\n';
	wrser << "-- VERSION 1.0" << '\n';

	for (auto& nsDef : doNsDefs) {
		// skip system namespaces
		if (nsDef.name.length() > 0 && nsDef.name[0] == '#') continue;

		wrser << "-- Dumping namespace '" << nsDef.name << "' ..." << '\n';

		wrser << "\\NAMESPACES ADD " << escapeName(nsDef.name) << " ";
		nsDef.GetJSON(wrser);
		wrser << '\n';

		vector<string> meta;
		err = db_.EnumMeta(nsDef.name, meta);
		if (err) return err;

		for (auto& mkey : meta) {
			string mdata;
			err = db_.GetMeta(nsDef.name, mkey, mdata);
			if (err) return err;

			wrser << "\\META PUT " << escapeName(nsDef.name) << " " << escapeName(mkey) << " " << escapeName(mdata) << '\n';
		}

		typename _DB::QueryResultsT itemResults;
		err = db_.Select(Query(nsDef.name), itemResults);

		if (!err.ok()) return err;

		for (auto it : itemResults) {
			if (!it.Status().ok()) return it.Status();
			wrser << "\\UPSERT " << escapeName(nsDef.name) << ' ';
			it.GetJSON(wrser, false);
			wrser << '\n';
			if (wrser.Len() > 0x100000) {
				output_() << wrser.Slice();
				wrser.Reset();
			}
		}
	}
	output_() << wrser.Slice();

	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::commandNamespaces(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "add")) {
		auto nsName = unescapeName(parser.NextToken());

		NamespaceDef def("");
		reindexer::Error err = def.FromJSON(const_cast<char*>(parser.CurPtr()));
		if (!err.ok()) {
			return Error(errParseJson, "Namespace structure is not valid - %s", err.what().c_str());
		}

		def.storage.DropOnFileFormatError(true);
		def.storage.CreateIfMissing(true);

		return db_.AddNamespace(def);
	} else if (iequals(subCommand, "list")) {
		vector<NamespaceDef> allNsDefs;

		auto err = db_.EnumNamespaces(allNsDefs, true);
		for (auto& ns : allNsDefs) {
			std::cout << ns.name << std::endl;
		}
		return err;

	} else if (iequals(subCommand, "drop")) {
		auto nsName = unescapeName(parser.NextToken());
		return db_.DropNamespace(nsName);
	}
	return Error(errParams, "Unknown sub command '%s' of namespaces command", subCommand.ToString().c_str());
}

template <typename _DB>
Error DBWrapper<_DB>::commandMeta(const string& command) {
	LineParser parser(command);
	parser.NextToken();
	string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "put")) {
		string nsName = unescapeName(parser.NextToken());
		string metaKey = unescapeName(parser.NextToken());
		string metaData = unescapeName(parser.NextToken());
		return db_.PutMeta(nsName, metaKey, metaData);
	} else if (iequals(subCommand, "list")) {
		auto nsName = unescapeName(parser.NextToken());
		vector<std::string> allMeta;
		auto err = db_.EnumMeta(nsName, allMeta);
		for (auto& metaKey : allMeta) {
			string metaData;
			db_.GetMeta(nsName, metaKey, metaData);
			std::cout << metaKey << " = " << metaData << std::endl;
		}
		return err;
	}
	return Error(errParams, "Unknown sub command '%s' of meta command", subCommand.ToString().c_str());
}

template <typename _DB>
Error DBWrapper<_DB>::commandHelp(const string& command) {
	LineParser parser(command);
	parser.NextToken();
	string_view subCommand = parser.NextToken();

	if (!subCommand.length()) {
		std::cout << "Available commands:\n\n";
		for (auto cmd : cmds_) {
			std::cout << "  " << std::left << std::setw(20) << cmd.command << "- " << cmd.description << std::endl;
		}
	} else {
		auto it = std::find_if(cmds_.begin(), cmds_.end(),
							   [&subCommand](const commandDefinition& def) { return iequals(def.command, subCommand); });

		if (it == cmds_.end()) {
			return Error(errParams, "Unknown command '%s' to help. To list of available command type '\\help'",
						 subCommand.ToString().c_str());
		}
		std::cout << it->command << " - " << it->description << ":" << std::endl << it->help << std::endl;
	}

	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::commandQuit(const string&) {
	terminate_ = true;
	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::commandSet(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	string_view variableName = parser.NextToken();
	string_view variableValue = parser.NextToken();

	variables_[variableName.ToString()] = variableValue.ToString();
	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::commandBench(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	int benchTime = stoi(parser.NextToken());
	if (benchTime == 0) benchTime = kBenchDefaultTime;

	db_.DropNamespace(kBenchNamespace);

	NamespaceDef nsDef(kBenchNamespace);
	nsDef.AddIndex("id", "hash", "int", IndexOpts().PK());

	auto err = db_.AddNamespace(nsDef);
	if (!err.ok()) return err;

	std::cout << "Seeding " << kBenchItemsCount << " documents to bench namespace..." << std::endl;
	for (int i = 0; i < kBenchItemsCount; i++) {
		auto item = db_.NewItem(kBenchNamespace);
		WrSerializer ser;
		JsonBuilder(ser).Put("id", i).Put("data", i);

		err = item.FromJSON(ser.c_str());
		if (!err.ok()) return err;

		err = db_.Upsert(kBenchNamespace, item);
		if (!err.ok()) return err;
	}
	std::cout << "done." << std::endl;

	std::cout << "Running " << benchTime << "s benchmark..." << std::endl;

	std::this_thread::sleep_for(std::chrono::seconds(1));

	auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(benchTime);
	std::atomic<int> count(0), errCount(0);

	auto worker = [this, deadline, &count, &errCount]() {
		for (; (count % 1000) || std::chrono::system_clock::now() < deadline; count++) {
			Query q(kBenchNamespace);
			q.Where(kBenchIndex, CondEq, count % kBenchItemsCount);
			auto results = new typename _DB::QueryResultsT;

			db_.Select(q, *results, [results, &errCount](const Error& err) {
				delete results;
				if (!err.ok()) errCount++;
			});
		}
	};
	auto threads = std::unique_ptr<std::thread[]>(new std::thread[numThreads_ - 1]);
	for (int i = 0; i < numThreads_ - 1; i++) threads[i] = std::thread(worker);
	worker();
	for (int i = 0; i < numThreads_ - 1; i++) threads[i].join();

	std::cout << "Done. Got " << count / benchTime << " QPS, " << errCount << " errors" << std::endl;

	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::queryResultsToJson(ostream& o, const typename _DB::QueryResultsT& r) {
	WrSerializer ser;
	size_t i = 0;
	bool prettyPrint = variables_[kVariableOutput] == kOutputModePretty;
	for (auto it : r) {
		Error err = it.GetJSON(ser, false);
		if (!err.ok()) return err;

		if (prettyPrint) {
			string json = ser.Slice().ToString();
			ser.Reset();
			prettyPrintJSON(json, ser);
		}
		ser << (++i == r.Count() ? "\n" : ",\n");
		if (ser.Len() > 0x100000 || prettyPrint) {
			o << ser.Slice();
			ser.Reset();
		}
	}
	o << ser.Slice();
	return errOK;
}

template <typename _DB>
bool DBWrapper<_DB>::Interactive() {
	bool wasError = false;
#if REINDEX_WITH_REPLXX
	replxx::Replxx rx;
	std::string history_file = reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), ".reindexer_history.txt");

	rx.history_load(history_file);
	rx.set_max_history_size(1000);
	rx.set_max_line_size(16384);
	rx.set_max_hint_rows(8);
	std::string prompt = "\x1b[1;32mReindexer\x1b[0m> ";

	// main repl loop
	while (!terminate_) {
		char const* input = nullptr;
		do {
			input = rx.input(prompt);
		} while (!input && errno == EAGAIN);

		if (input == nullptr) break;

		if (!*input) continue;

		Error err = ProcessCommand(input);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			wasError = true;
		}

		rx.history_add(input);
	}
	rx.history_save(history_file);
#else
	std::string prompt = "Reindexer> ";
	// main repl loop
	while (!terminate_) {
		std::string command;
		std::cout << prompt;
		if (!std::getline(std::cin, command)) break;
		Error err = ProcessCommand(command);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			wasError = true;
		}
	}
#endif
	return !wasError;
}

template <typename _DB>
bool DBWrapper<_DB>::FromFile() {
	bool wasError = false;
	std::ifstream infile(fileName_);
	if (!infile) {
		std::cerr << "ERROR: Can't open " << fileName_ << std::endl;
		return false;
	}

	std::string line;
	while (std::getline(infile, line)) {
		Error err = ProcessCommand(line);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			wasError = true;
		}
	}
	return !wasError;
}

template <typename _DB>
bool DBWrapper<_DB>::Run() {
	auto err = output_.Status();
	if (!err.ok()) {
		if (!err.ok()) {
			std::cerr << "Output error: " << err.what() << std::endl;
			return false;
		}
	}

	if (!command_.empty()) {
		err = ProcessCommand(command_);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			return false;
		}
		return true;
	}
	if (!fileName_.empty()) {
		return FromFile();
	} else {
		return Interactive();
	}
}

template class DBWrapper<reindexer::client::Reindexer>;
template class DBWrapper<reindexer::Reindexer>;

}  // namespace reindexer_tool
