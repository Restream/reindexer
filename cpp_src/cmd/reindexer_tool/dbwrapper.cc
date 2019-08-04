#include "dbwrapper.h"
#include <iomanip>
#include <thread>
#include "client/reindexer.h"
#include "core/reindexer.h"
#include "replicator/walrecord.h"
#if REINDEX_WITH_REPLXX
#include "replxx.hxx"
#endif

#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
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
	return Error(errParams, "Unknown command '%s'. Type '\\help' to list of available commands", token);
}

template <typename _DB>
Error DBWrapper<_DB>::commandSelect(const string& command) {
	typename _DB::QueryResultsT results(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	Query q;
	try {
		q.FromSQL(command);
	} catch (const Error& err) {
		return err;
	}
	bool isWALQuery = q.entries.Size() == 1 && q.entries.IsEntry(0) && q.entries[0].index == "#lsn";

	auto err = db_.Select(q, results);

	if (err.ok()) {
		if (results.Count()) {
			output_() << "[" << std::endl;
			err = queryResultsToJson(output_(), results, isWALQuery);
			output_() << "]" << std::endl;
		}

		string explain = results.GetExplainResults();
		if (!explain.empty()) {
			output_() << "Explain: " << std::endl;
			if (variables_[kVariableOutput] == kOutputModePretty) {
				WrSerializer ser;
				prettyPrintJSON(reindexer::giftStr(explain), ser);
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
					assert(!agg.fields.empty());
					reindexer::h_vector<int, 1> maxW;
					maxW.reserve(agg.fields.size());
					for (const auto& field : agg.fields) {
						maxW.push_back(field.length());
					}
					for (auto& row : agg.facets) {
						assert(row.values.size() == agg.fields.size());
						for (size_t i = 0; i < row.values.size(); ++i) {
							maxW.at(i) = std::max(maxW.at(i), int(row.values[i].length()));
						}
					}
					int rowWidth = 8 + (maxW.size() - 1) * 2;
					for (auto& mW : maxW) {
						mW += 3;
						rowWidth += mW;
					}
					for (size_t i = 0; i < agg.fields.size(); ++i) {
						if (i != 0) output_() << "| ";
						output_() << std::left << std::setw(maxW.at(i)) << agg.fields[i];
					}
					output_() << "| count" << std::endl;
					output_() << std::left << std::setw(rowWidth) << std::setfill('-') << "" << std::endl << std::setfill(' ');
					for (auto& row : agg.facets) {
						for (size_t i = 0; i < row.values.size(); ++i) {
							if (i != 0) output_() << "| ";
							output_() << std::left << std::setw(maxW.at(i)) << row.values[i];
						}
						output_() << "| " << row.count << std::endl;
					}
				} else {
					assert(agg.fields.size() == 1);
					output_() << agg.aggTypeToStr(agg.type) << "(" << agg.fields.front() << ") = " << agg.value << std::endl;
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
Error DBWrapper<_DB>::commandUpdateSQL(const string& command) {
	typename _DB::QueryResultsT results;
	Query q;
	try {
		q.FromSQL(command);
	} catch (const Error& err) {
		return err;
	}
	auto err = db_.Update(q, results);

	if (err.ok()) {
		output_() << "Updated " << results.Count() << " documents" << std::endl;
	}
	return err;
}

template <typename _DB>
Error DBWrapper<_DB>::commandUpsert(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	string nsName = reindexer::unescapeString(parser.NextToken());

	auto item = new typename _DB::ItemT(db_.NewItem(nsName));

	Error status = item->Status();
	if (!status.ok()) {
		delete item;
		return status;
	}

	status = item->Unsafe().FromJSON(parser.CurPtr());
	if (!status.ok()) {
		delete item;
		return status;
	}

	return db_.WithCompletion([item](const Error& /*err*/) { delete item; }).Upsert(nsName, *item);
}

template <typename _DB>
Error DBWrapper<_DB>::commandDelete(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	auto nsName = reindexer::unescapeString(parser.NextToken());

	auto item = db_.NewItem(nsName);
	if (!item.Status().ok()) return item.Status();

	auto err = item.Unsafe().FromJSON(parser.CurPtr());
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

		wrser << "\\NAMESPACES ADD " << reindexer::escapeString(nsDef.name) << " ";
		nsDef.GetJSON(wrser);
		wrser << '\n';

		vector<string> meta;
		err = db_.EnumMeta(nsDef.name, meta);
		if (err) return err;

		for (auto& mkey : meta) {
			string mdata;
			err = db_.GetMeta(nsDef.name, mkey, mdata);
			if (err) return err;

			wrser << "\\META PUT " << reindexer::escapeString(nsDef.name) << " " << reindexer::escapeString(mkey) << " "
				  << reindexer::escapeString(mdata) << '\n';
		}

		typename _DB::QueryResultsT itemResults;
		err = db_.Select(Query(nsDef.name), itemResults);

		if (!err.ok()) return err;

		for (auto it : itemResults) {
			if (!it.Status().ok()) return it.Status();
			wrser << "\\UPSERT " << reindexer::escapeString(nsDef.name) << ' ';
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
		auto nsName = reindexer::unescapeString(parser.NextToken());

		NamespaceDef def("");
		reindexer::Error err = def.FromJSON(reindexer::giftStr(parser.CurPtr()));
		if (!err.ok()) {
			return Error(errParseJson, "Namespace structure is not valid - %s", err.what());
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
		auto nsName = reindexer::unescapeString(parser.NextToken());
		return db_.DropNamespace(nsName);
	}
	return Error(errParams, "Unknown sub command '%s' of namespaces command", subCommand);
}

template <typename _DB>
Error DBWrapper<_DB>::commandMeta(const string& command) {
	LineParser parser(command);
	parser.NextToken();
	string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "put")) {
		string nsName = reindexer::unescapeString(parser.NextToken());
		string metaKey = reindexer::unescapeString(parser.NextToken());
		string metaData = reindexer::unescapeString(parser.NextToken());
		return db_.PutMeta(nsName, metaKey, metaData);
	} else if (iequals(subCommand, "list")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		vector<std::string> allMeta;
		auto err = db_.EnumMeta(nsName, allMeta);
		for (auto& metaKey : allMeta) {
			string metaData;
			db_.GetMeta(nsName, metaKey, metaData);
			std::cout << metaKey << " = " << metaData << std::endl;
		}
		return err;
	}
	return Error(errParams, "Unknown sub command '%s' of meta command", subCommand);
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
			return Error(errParams, "Unknown command '%s' to help. To list of available command type '\\help'", subCommand);
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

	variables_[string(variableName)] = string(variableValue);
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

		err = item.FromJSON(ser.Slice());
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

			db_.WithCompletion([results, &errCount](const Error& err) {
				   delete results;
				   if (!err.ok()) errCount++;
			   })
				.Select(q, *results);
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
Error DBWrapper<_DB>::commandSubscribe(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	bool on = !iequals(parser.NextToken(), "off");

	return db_.SubscribeUpdates(this, on);
}

template <typename _DB>
void DBWrapper<_DB>::OnWALUpdate(int64_t lsn, string_view nsName, const reindexer::WALRecord& wrec) {
	WrSerializer ser;
	ser << "#" << lsn << " " << nsName << " ";
	wrec.Dump(ser, [this, nsName](string_view cjson) {
		auto item = db_.NewItem(nsName);
		item.FromCJSON(cjson);
		return string(item.GetJSON());
	});
	output_() << ser.Slice() << std::endl;
}

template <typename _DB>
void DBWrapper<_DB>::OnConnectionState(const Error& err) {
	if (err.ok())
		output_() << "[OnConnectionState] connected" << std::endl;
	else
		output_() << "[OnConnectionState] closed, reason: " << err.what() << std::endl;
}

template <typename _DB>
Error DBWrapper<_DB>::queryResultsToJson(ostream& o, const typename _DB::QueryResultsT& r, bool isWALQuery) {
	WrSerializer ser;
	size_t i = 0;
	bool prettyPrint = variables_[kVariableOutput] == kOutputModePretty;
	for (auto it : r) {
		if (isWALQuery) ser << '#' << it.GetLSN() << ' ';

		if (it.IsRaw()) {
			reindexer::WALRecord rec(it.GetRaw());
			rec.Dump(ser, [this, &r](string_view cjson) {
				auto item = db_.NewItem(r.GetNamespaces()[0]);
				item.FromCJSON(cjson);
				return string(item.GetJSON());
			});
		} else {
			if (isWALQuery) ser << "WalItemUpdate ";
			Error err = it.GetJSON(ser, false);
			if (!err.ok()) return err;

			if (prettyPrint) {
				string json(ser.Slice());
				ser.Reset();
				prettyPrintJSON(reindexer::giftStr(json), ser);
			}
		}
		if ((++i != r.Count()) && !isWALQuery) ser << ',';
		ser << '\n';
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
	// rx.set_max_line_size(16384);
	rx.set_max_hint_rows(8);
	rx.set_completion_callback([this](std::string const& input, int /*pos*/) -> replxx::Replxx::completions_t {
		replxx::Replxx::completions_t completions;
		db_.GetSqlSuggestions(input, input.empty() ? 0 : input.length() - 1, completions);
		return completions;
	});

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
