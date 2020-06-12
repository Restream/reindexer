#include "commandsprocessor.h"
#include <csignal>
#include <functional>
#include <thread>
#include "client/reindexer.h"
#include "core/reindexer.h"
#include "replicator/walrecord.h"
#if REINDEX_WITH_REPLXX
#include "replxx.hxx"
#endif

#include <iomanip>
#include <iostream>
#include "core/cjson/jsonbuilder.h"
#include "core/queryresults/tableviewbuilder.h"
#include "tableviewscroller.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/stringstools.h"
#include "vendor/gason/gason.h"

using std::vector;
using std::unordered_map;
using std::string;
using std::list;

namespace reindexer_tool {

using reindexer::iequals;
using reindexer::WrSerializer;
using reindexer::NamespaceDef;
using reindexer::JsonBuilder;
using reindexer::Query;

const string kConfigFile = "rxtool_settings.txt";

const string kVariableOutput = "output";
const string kOutputModeJson = "json";
const string kOutputModeTable = "table";
const string kOutputModePretty = "pretty";
const string kOutputModePrettyCollapsed = "collapsed";
const string kBenchNamespace = "rxtool_bench";
const string kBenchIndex = "id";

const int kBenchItemsCount = 10000;
const int kBenchDefaultTime = 5;

static std::function<void(int)> sigIntHandler;

void sigint_handler(int signal) {
	if (sigIntHandler) sigIntHandler(signal);
}

template <typename DBInterface>
template <typename... Args>
Error CommandsProcessor<DBInterface>::Connect(const string& dsn, Args... args) {
	string outputMode;
	if (reindexer::fs::ReadFile(reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), kConfigFile), outputMode) > 0) {
		gason::JsonParser jsonParser;
		gason::JsonNode value = jsonParser.Parse(reindexer::giftStr(outputMode));
		for (auto node : value) {
			WrSerializer ser;
			reindexer::jsonValueToString(node.value, ser, 0, 0, false);
			variables_[kVariableOutput] = string(ser.Slice());
		}
	}
	if (variables_.empty()) {
		variables_[kVariableOutput] = kOutputModeJson;
	}
	if (!uri_.parse(dsn)) {
		return Error(errNotValid, "Cannot connect to DB: Not a valid uri");
	}
	Error err = db_.Connect(dsn, args...);
	if (err.ok()) {
		sigIntHandler = std::bind(&CommandsProcessor::onSigInt, this, std::placeholders::_1);
		signal(SIGINT, sigint_handler);
	}
	return err;
}

template <typename DBInterface>
CommandsProcessor<DBInterface>::~CommandsProcessor() {
	stop();
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::onSigInt(int) {
	cancelCtx_.Cancel();
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::Process(string command) {
	LineParser parser(command);
	auto token = parser.NextToken();

	if (!token.length() || token.substr(0, 2) == "--") return errOK;

	cancelCtx_.Reset();
	for (auto& c : cmds_) {
		if (iequals(token, c.command)) {
			return (this->*(c.handler))(command);
		}
	}
	return Error(errParams, "Unknown command '%s'. Type '\\help' to list of available commands", token);
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandSelect(const string& command) {
	typename DBInterface::QueryResultsT results(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	Query q;
	try {
		q.FromSQL(command);
	} catch (const Error& err) {
		return err;
	}

	auto err = db_.Select(q, results);

	if (err.ok()) {
		if (results.Count()) {
			string outputType = variables_[kVariableOutput];
			if (outputType == kOutputModeTable) {
				auto isCanceled = [this]() -> bool { return cancelCtx_.IsCancelled(); };
				reindexer::TableViewBuilder<typename DBInterface::QueryResultsT> tableResultsBuilder(results);
				if (outFileName_.empty() && !reindexer::isStdoutRedirected()) {
					TableViewScroller<typename DBInterface::QueryResultsT> resultsScroller(results, tableResultsBuilder,
																						   reindexer::getTerminalSize().height - 1);
					resultsScroller.Scroll(output_, isCanceled);
				} else {
					tableResultsBuilder.Build(output_(), isCanceled);
				}
			} else {
				output_() << "[" << std::endl;
				err = queryResultsToJson(output_(), results, q.IsWALQuery());
				output_() << "]" << std::endl;
			}
		}

		string explain = results.GetExplainResults();
		if (!explain.empty() && !cancelCtx_.IsCancelled()) {
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
		if (aggResults.size() && !cancelCtx_.IsCancelled()) {
			output_() << "Aggregations: " << std::endl;
			for (auto& agg : aggResults) {
				switch (agg.type) {
					case AggFacet: {
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
					} break;
					case AggDistinct:
						assert(agg.fields.size() == 1);
						output_() << "Distinct (" << agg.fields.front() << ")" << std::endl;
						for (auto& v : agg.distincts) {
							output_() << v << std::endl;
						}
						output_() << "Returned " << agg.distincts.size() << " values" << std::endl;
						break;
					default:
						assert(agg.fields.size() == 1);
						output_() << agg.aggTypeToStr(agg.type) << "(" << agg.fields.front() << ") = " << agg.value << std::endl;
				}
			}
		}
	}
	return err;
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandDeleteSQL(const string& command) {
	typename DBInterface::QueryResultsT results;
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

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandUpdateSQL(const string& command) {
	typename DBInterface::QueryResultsT results;
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

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandUpsert(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	string nsName = reindexer::unescapeString(parser.NextToken());

	auto item = new typename DBInterface::ItemT(db_.NewItem(nsName));

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

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandDelete(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	auto nsName = reindexer::unescapeString(parser.NextToken());

	auto item = db_.NewItem(nsName);
	if (!item.Status().ok()) return item.Status();

	auto err = item.Unsafe().FromJSON(parser.CurPtr());
	if (!err.ok()) return err;

	return db_.Delete(nsName, item);
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandDump(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	vector<NamespaceDef> allNsDefs, doNsDefs;

	auto err = db_.EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts());
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
		// skip system namespaces, except #config
		if (nsDef.name.length() > 0 && nsDef.name[0] == '#' && nsDef.name != "#config") continue;

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

		typename DBInterface::QueryResultsT itemResults;
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

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandNamespaces(const string& command) {
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

		err = db_.OpenNamespace(def.name);
		if (!err.ok()) {
			return err;
		}
		for (auto& idx : def.indexes) {
			err = db_.AddIndex(def.name, idx);
			if (!err.ok()) {
				return err;
			}
		}
		return errOK;

	} else if (iequals(subCommand, "list")) {
		vector<NamespaceDef> allNsDefs;

		auto err = db_.EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts().WithClosed());
		for (auto& ns : allNsDefs) {
			std::cout << ns.name << std::endl;
		}
		return err;

	} else if (iequals(subCommand, "drop")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		return db_.DropNamespace(nsName);
	} else if (iequals(subCommand, "truncate")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		return db_.TruncateNamespace(nsName);
	} else if (iequals(subCommand, "rename")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		auto nsNewName = reindexer::unescapeString(parser.NextToken());
		return db_.RenameNamespace(nsName, nsNewName);
	}
	return Error(errParams, "Unknown sub command '%s' of namespaces command", subCommand);
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandMeta(const string& command) {
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

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandHelp(const string& command) {
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

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandQuit(const string&) {
	terminate_ = true;
	return errOK;
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandSet(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	string_view variableName = parser.NextToken();
	string_view variableValue = parser.NextToken();

	variables_[string(variableName)] = string(variableValue);

	WrSerializer wrser;
	reindexer::JsonBuilder configBuilder(wrser);
	for (auto it = variables_.begin(); it != variables_.end(); ++it) {
		configBuilder.Put(it->first, it->second);
	}
	configBuilder.End();
	reindexer::fs::WriteFile(reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), kConfigFile), wrser.Slice());

	return errOK;
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandBench(const string& command) {
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
			auto results = new typename DBInterface::QueryResultsT;

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

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::commandSubscribe(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	bool on = !iequals(parser.NextToken(), "off");

	return db_.SubscribeUpdates(this, on);
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::OnWALUpdate(int64_t lsn, string_view nsName, const reindexer::WALRecord& wrec) {
	WrSerializer ser;
	ser << "#" << lsn << " " << nsName << " ";
	wrec.Dump(ser, [this, nsName](string_view cjson) {
		auto item = db_.NewItem(nsName);
		item.FromCJSON(cjson);
		return string(item.GetJSON());
	});
	output_() << ser.Slice() << std::endl;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::OnConnectionState(const Error& err) {
	if (err.ok())
		output_() << "[OnConnectionState] connected" << std::endl;
	else
		output_() << "[OnConnectionState] closed, reason: " << err.what() << std::endl;
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::queryResultsToJson(ostream& o, const typename DBInterface::QueryResultsT& r, bool isWALQuery) {
	if (cancelCtx_.IsCancelled()) return errOK;
	WrSerializer ser;
	size_t i = 0;
	bool scrollable = outFileName_.empty() && !reindexer::isStdoutRedirected();
	reindexer::TerminalSize terminalSize;
	if (scrollable) {
		terminalSize = reindexer::getTerminalSize();
		scrollable = (int(r.Count()) > terminalSize.height);
	}
	bool prettyPrint = variables_[kVariableOutput] == kOutputModePretty;
	for (auto it : r) {
		if (cancelCtx_.IsCancelled()) break;
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
		if ((ser.Len() > 0x100000) || prettyPrint || scrollable) {
			if (scrollable && (i % (terminalSize.height - 1) == 0)) {
				WaitEnterToContinue(o, terminalSize.width, [this]() -> bool { return cancelCtx_.IsCancelled(); });
			}
			o << ser.Slice();
			ser.Reset();
		}
	}
	if (!cancelCtx_.IsCancelled()) {
		o << ser.Slice();
	}
	return errOK;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::checkForNsNameMatch(string_view str, std::vector<string>& suggestions) {
	vector<NamespaceDef> allNsDefs;
	Error err = db_.EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts().WithClosed());
	if (!err.ok()) return;
	for (auto& ns : allNsDefs) {
		if (str.empty() || reindexer::isBlank(str) || ((str.length() < ns.name.length()) && reindexer::checkIfStartsWith(str, ns.name))) {
			suggestions.emplace_back(ns.name);
		}
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::checkForCommandNameMatch(string_view str, std::initializer_list<string_view> cmds,
															  std::vector<string>& suggestions) {
	for (string_view cmd : cmds) {
		if (str.empty() || reindexer::isBlank(str) || ((str.length() < cmd.length()) && reindexer::checkIfStartsWith(str, cmd))) {
			suggestions.emplace_back(cmd);
		}
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::addCommandsSuggestions(std::string const& cmd, std::vector<string>& suggestions) {
	LineParser parser(cmd);
	string_view token = parser.NextToken();

	if ((token == "\\upsert") || (token == "\\delete")) {
		token = parser.NextToken();
		if (parser.End()) {
			checkForNsNameMatch(token, suggestions);
		}
	} else if ((token == "\\dump") && !parser.End()) {
		while (!parser.End()) {
			checkForNsNameMatch(parser.NextToken(), suggestions);
		}
	} else if (token == "\\namespaces") {
		token = parser.NextToken();
		if (token == "drop") {
			checkForNsNameMatch(parser.NextToken(), suggestions);
		} else {
			checkForCommandNameMatch(token, {"add", "list", "drop"}, suggestions);
		}
	} else if (token == "\\meta") {
		checkForCommandNameMatch(parser.NextToken(), {"put", "list"}, suggestions);
	} else if (token == "\\set") {
		token = parser.NextToken();
		if (token == "output") {
			checkForCommandNameMatch(parser.NextToken(), {"json", "pretty", "table"}, suggestions);
		} else {
			checkForCommandNameMatch(token, {"output"}, suggestions);
		}
	} else if (token == "\\subscribe") {
		checkForCommandNameMatch(parser.NextToken(), {"on", "off"}, suggestions);
	} else if (token == "\\databases") {
		token = parser.NextToken();
		if (token == "use") {
			vector<string> dbList;
			Error err = getAvailableDatabases(dbList);
			if (err.ok()) {
				token = parser.NextToken();
				for (const string& dbName : dbList) {
					if (token.empty() || reindexer::isBlank(token) ||
						((token.length() < dbName.length()) && reindexer::checkIfStartsWith(token, dbName))) {
						suggestions.emplace_back(dbName);
					}
				}
			}
		} else {
			checkForCommandNameMatch(token, {"use", "list"}, suggestions);
		}
	} else {
		for (const commandDefinition& cmdDef : cmds_) {
			if (token.empty() || reindexer::isBlank(token) ||
				((token.length() < cmdDef.command.length()) && reindexer::checkIfStartsWith(token, cmdDef.command))) {
				suggestions.emplace_back(cmdDef.command[0] == '\\' ? cmdDef.command.substr(1) : cmdDef.command);
			}
		}
	}
}

template <typename DBInterface>
template <typename T>
void CommandsProcessor<DBInterface>::setCompletionCallback(T& rx, void (T::*set_completion_callback)(new_v_callback_t const&)) {
	(rx.*set_completion_callback)([this](std::string const& input, int) -> replxx::Replxx::completions_t {
		std::vector<string> completions;
		if (!input.empty() && input[0] != '\\') db_.GetSqlSuggestions(input, input.length() - 1, completions);
		if (completions.empty()) {
			addCommandsSuggestions(input, completions);
		}
		replxx::Replxx::completions_t result;
		for (const string& suggestion : completions) result.emplace_back(suggestion);
		return result;
	});
}

template <typename DBInterface>
template <typename T>
void CommandsProcessor<DBInterface>::setCompletionCallback(T& rx, void (T::*set_completion_callback)(old_v_callback_t const&, void*)) {
	(rx.*set_completion_callback)(
		[this](std::string const& input, int, void*) -> replxx::Replxx::completions_t {
			std::vector<string> completions;
			if (!input.empty() && input[0] != '\\') db_.GetSqlSuggestions(input, input.length() - 1, completions);
			if (completions.empty()) {
				addCommandsSuggestions(input, completions);
			}
			return completions;
		},
		nullptr);
}

template <typename T>
class HasSetMaxLineSize {
private:
	typedef char YesType[1], NoType[2];

	template <typename C>
	static YesType& test(decltype(&C::set_max_line_size));
	template <typename C>
	static NoType& test(...);

public:
	enum { value = sizeof(test<T>(0)) == sizeof(YesType) };
};

template <class T>
void setMaxLineSize(T* rx, int arg, typename std::enable_if<HasSetMaxLineSize<T>::value>::type* = 0) {
	return rx->set_max_line_size(arg);
}

void setMaxLineSize(...) {}

template <typename DBInterface>
bool CommandsProcessor<DBInterface>::Interactive() {
	bool wasError = false;
#if REINDEX_WITH_REPLXX
	replxx::Replxx rx;
	std::string history_file = reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), ".reindexer_history.txt");

	setMaxLineSize(&rx, 0x10000);
	rx.history_load(history_file);
	rx.set_max_history_size(1000);
	rx.set_max_hint_rows(8);
	setCompletionCallback(rx, &replxx::Replxx::set_completion_callback);

	std::string prompt = "\x1b[1;32mReindexer\x1b[0m> ";

	// main repl loop
	while (!terminate_) {
		char const* input = nullptr;
		do {
			input = rx.input(prompt);
		} while (!input && errno == EAGAIN);

		if (input == nullptr) break;

		if (!*input) continue;

		Error err = Process(input);
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

template <typename DBInterface>
bool CommandsProcessor<DBInterface>::FromFile(std::istream& infile) {
	bool wasError = false;
	if (!infile) {
		std::cerr << "ERROR: Can't open " << inFileName_ << std::endl;
		return false;
	}

	std::string line;
	while (std::getline(infile, line)) {
		Error err = Process(line);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			if (!wasError && !db_.Status().ok()) {
				return false;
			}
			wasError = true;
		}
	}
	return !wasError;
}

template <typename DBInterface>
bool CommandsProcessor<DBInterface>::Run() {
	auto err = output_.Status();
	if (!err.ok()) {
		std::cerr << "Output error: " << err.what() << std::endl;
		return false;
	}

	if (!command_.empty()) {
		err = Process(command_);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			return false;
		}
		return true;
	}

	if (!inFileName_.empty()) {
		std::ifstream infile(inFileName_);
		return FromFile(infile);
	} else if (reindexer::isStdinRedirected()) {
		return FromFile(std::cin);
	} else {
		return Interactive();
	}
}

template <typename DBInterface>
string CommandsProcessor<DBInterface>::getCurrentDsn() const {
	string dsn(uri_.scheme() + "://");
	if (!uri_.password().empty() && !uri_.username().empty()) {
		dsn += uri_.username() + ":" + uri_.password() + "@";
	}
	dsn += uri_.hostname() + ":" + uri_.port() + "/";
	return dsn;
}

template <>
Error CommandsProcessor<reindexer::client::Reindexer>::stop() {
	return db_.Stop();
}

template <>
Error CommandsProcessor<reindexer::Reindexer>::stop() {
	return Error();
}

template <>
Error CommandsProcessor<reindexer::client::Reindexer>::getAvailableDatabases(vector<string>& dbList) {
	return db_.EnumDatabases(dbList);
}

template <>
Error CommandsProcessor<reindexer::Reindexer>::getAvailableDatabases(vector<string>&) {
	return Error();
}

template <>
Error CommandsProcessor<reindexer::client::Reindexer>::commandProcessDatabases(const string& command) {
	LineParser parser(command);
	parser.NextToken();
	string_view subCommand = parser.NextToken();
	assert(uri_.scheme() == "cproto");
	if (subCommand == "list") {
		vector<string> dbList;
		Error err = getAvailableDatabases(dbList);
		if (!err.ok()) return err;
		for (const string& dbName : dbList) std::cout << dbName << std::endl;
		return Error();
	} else if (subCommand == "use") {
		string currentDsn = getCurrentDsn() + std::string(parser.NextToken());
		Error err = stop();
		if (!err.ok()) return err;
		err = db_.Connect(currentDsn);
		if (err.ok()) err = db_.Status();
		if (err.ok()) std::cout << "Succesfully connected to " << currentDsn << std::endl;
		return err;
	} else if (subCommand == "create") {
		auto dbName = parser.NextToken();
		string currentDsn = getCurrentDsn() + std::string(dbName);
		Error err = stop();
		if (!err.ok()) return err;
		std::cout << "Creating database '" << dbName << "'" << std::endl;
		err = db_.Connect(currentDsn, reindexer::client::ConnectOpts().CreateDBIfMissing());
		if (!err.ok()) {
			std::cout << "Error on database '" << dbName << "' creation" << std::endl;
			return err;
		}
		std::vector<std::string> dbNames;
		err = db_.EnumDatabases(dbNames);
		if (std::find(dbNames.begin(), dbNames.end(), std::string(dbName)) != dbNames.end()) {
			std::cout << "Succesfully created database '" << dbName << "'" << std::endl;
		} else {
			std::cout << "Error on database '" << dbName << "' creation" << std::endl;
		}
		return err;
	}
	return Error(errNotValid, "Invalid command");
}

template <>
Error CommandsProcessor<reindexer::Reindexer>::commandProcessDatabases(const string& command) {
	(void)command;
	return Error(errNotValid, "Database processing commands are not supported in builtin mode");
}

template class CommandsProcessor<reindexer::client::Reindexer>;
template class CommandsProcessor<reindexer::Reindexer>;
template Error CommandsProcessor<reindexer::Reindexer>::Connect(const string& dsn);
template Error CommandsProcessor<reindexer::client::Reindexer>::Connect(const string& dsn);
template Error CommandsProcessor<reindexer::client::Reindexer>::Connect(const string& dsn, reindexer::client::ConnectOpts opts);

}  // namespace reindexer_tool
